namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public class DwhTableBuilder
    {
        public DwhBuilder DwhBuilder { get; }
        public ResilientTable Table { get; }
        public SqlTable SqlTable { get; }

        private readonly List<Func<DwhTableBuilder, IEnumerable<IExecutable>>> _finalizerCreators = new List<Func<DwhTableBuilder, IEnumerable<IExecutable>>>();
        private readonly List<Func<DwhTableBuilder, IEnumerable<IRowOperation>>> _operationCreators = new List<Func<DwhTableBuilder, IEnumerable<IRowOperation>>>();
        private Func<IEvaluable> _inputProcessCreator;

        public DwhTableBuilder(DwhBuilder builder, ResilientTable table, SqlTable sqlTable)
        {
            DwhBuilder = builder;
            Table = table;
            SqlTable = sqlTable;
        }

        internal void AddOperationCreator(Func<DwhTableBuilder, IEnumerable<IRowOperation>> creator)
        {
            _operationCreators.Add(creator);
        }

        internal void AddFinalizerCreator(Func<DwhTableBuilder, IEnumerable<IExecutable>> creator)
        {
            _finalizerCreators.Add(creator);
        }

        internal void SetInputProcessCreator(Func<IEvaluable> creator)
        {
            _inputProcessCreator = creator;
        }

        internal void Build()
        {
            Table.FinalizerCreator = _ => TableFinalizerCreator();
            Table.MainProcessCreator = t => CreateTableMainProcess();
        }

        private DwhTableBuilder AddBooleanConversionOperation()
        {
            var boolColumns = SqlTable.Columns
                .Where(col => col.Type == SqlType.Boolean)
                .Select(col => col.Name)
                .ToList();

            if (boolColumns.Count == 0)
                return this;

            _operationCreators.Add(builder => new[]{
                new CustomOperation()
                {
                    InstanceName = "ConvertBooleans",
                    Then = (op, row) =>
                    {
                        foreach (var col in boolColumns)
                        {
                            if (!row.IsNull(col))
                            {
                                var value = row[col];
                                if (value is bool boolv)
                                    row.SetValue(col, boolv, op);
                                else if (value is byte bv)
                                    row.SetValue(col, bv == 1, op);
                                else if (value is int iv)
                                    row.SetValue(col, iv == 1, op);
                                else
                                    throw new InvalidValueException(op.Process, null, row, col);
                            }
                        }
                    }
                }
            });

            return this;
        }

        private IRowOperation CreateTempWriterOperation(ResilientTable table, SqlTable sqlTable, bool bulkCopyCheckConstraints = false)
        {
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var isIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());

            var tempColumns = sqlTable.Columns.Select(x => x.Name);
            if (isIdentity)
            {
                var pkColumnNames = pk.SqlColumns.Select(x => x.SqlColumn.Name).ToHashSet();
                tempColumns = tempColumns.Where(x => !pkColumnNames.Contains(x));
            }

            if (DwhBuilder.Configuration.UseEtlRunTable)
            {
                tempColumns = tempColumns.Where(x => x != DwhBuilder.Configuration.EtlInsertRunIdColumnName && x != DwhBuilder.Configuration.EtlUpdateRunIdColumnName);
            }

            return new MsSqlWriteToTableWithMicroTransactionsOperation()
            {
                InstanceName = "Write",
                ConnectionString = table.Scope.Configuration.ConnectionString,
                BulkCopyCheckConstraints = bulkCopyCheckConstraints,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = tempColumns
                    .Select(column => new DbColumnDefinition(column))
                    .ToArray(),
                },
            };
        }

        private IEnumerable<IExecutable> CreateTableMainProcess()
        {
            AddBooleanConversionOperation();

            var currentRunid = DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var operations = new List<IRowOperation>();

            foreach (var operationCreator in _operationCreators)
            {
                operations.AddRange(operationCreator?.Invoke(this));
            }

            operations.Add(CreateTempWriterOperation(Table, SqlTable));

            yield return new OperationHostProcess(Table.Scope.Context, DwhBuilder.ConnectionString.Unescape(Table.TableName))
            {
                InputProcess = _inputProcessCreator?.Invoke(),
                Operations = operations,
            };
        }

        private IEnumerable<IExecutable> TableFinalizerCreator()
        {
            foreach (var creator in _finalizerCreators)
            {
                foreach (var finalizer in creator.Invoke(this))
                {
                    yield return finalizer;
                }
            }
        }
    }
}