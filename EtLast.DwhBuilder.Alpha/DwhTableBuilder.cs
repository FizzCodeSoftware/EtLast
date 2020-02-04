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

        public SqlColumn RecordTimestampIndicatorColumn { get; }
        public string EtlInsertRunIdColumnNameEscaped { get; }
        public string EtlUpdateRunIdColumnNameEscaped { get; }

        public bool BaseHasValidToColumn { get; }
        public string ValidFromColumnName { get; }
        public string ValidToColumnName { get; }
        public string ValidFromColumnNameEscaped { get; }
        public string ValidToColumnNameEscaped { get; }

        private readonly List<Func<DwhTableBuilder, IEnumerable<IExecutable>>> _finalizerCreators = new List<Func<DwhTableBuilder, IEnumerable<IExecutable>>>();
        private readonly List<Func<DwhTableBuilder, IEnumerable<IRowOperation>>> _operationCreators = new List<Func<DwhTableBuilder, IEnumerable<IRowOperation>>>();
        private Func<IEvaluable> _inputProcessCreator;

        public DwhTableBuilder(DwhBuilder builder, ResilientTable table, SqlTable sqlTable)
        {
            DwhBuilder = builder;
            Table = table;
            SqlTable = sqlTable;
            RecordTimestampIndicatorColumn = SqlTable.Columns.FirstOrDefault(x => x.HasProperty<RecordTimestampIndicatorColumnProperty>());

            EtlInsertRunIdColumnNameEscaped = SqlTable.Columns
                .Where(x => string.Equals(x.Name, builder.Configuration.EtlInsertRunIdColumnName, StringComparison.InvariantCultureIgnoreCase))
                .Select(x => builder.ConnectionString.Escape(x.Name))
                .FirstOrDefault();

            EtlUpdateRunIdColumnNameEscaped = SqlTable.Columns
                .Where(x => string.Equals(x.Name, builder.Configuration.EtlUpdateRunIdColumnName, StringComparison.InvariantCultureIgnoreCase))
                .Select(x => builder.ConnectionString.Escape(x.Name))
                .FirstOrDefault();

            ValidFromColumnName = SqlTable.Columns
                .Where(x => string.Equals(x.Name, builder.Configuration.ValidFromColumnName, StringComparison.InvariantCultureIgnoreCase))
                .Select(x => x.Name)
                .FirstOrDefault();

            ValidFromColumnNameEscaped = ValidFromColumnName != null ? builder.ConnectionString.Escape(ValidFromColumnName) : null;

            BaseHasValidToColumn = SqlTable.Columns
                .Any(x => string.Equals(x.Name, builder.Configuration.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase));

            ValidToColumnName = ValidFromColumnName != null ? builder.Configuration.ValidToColumnName : null;
            ValidToColumnNameEscaped = ValidToColumnName != null ? builder.ConnectionString.Escape(ValidToColumnName) : null;
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
            Table.FinalizerCreator = _ => CreateTableFinalizers();
            Table.MainProcessCreator = t => CreateTableMainProcess();
        }

        private IRowOperation CreateTempWriterOperation(ResilientTable table, SqlTable sqlTable, bool bulkCopyCheckConstraints = false)
        {
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var okIsIdentity = pk.SqlColumns.Any(c => c.SqlColumn.HasProperty<Identity>());

            var tempColumns = sqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>());

            if (okIsIdentity)
            {
                tempColumns = tempColumns
                    .Where(x => pk.SqlColumns.All(pkc => !string.Equals(pkc.SqlColumn.Name, x.Name, StringComparison.InvariantCultureIgnoreCase)));
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
                        .Select(c => new DbColumnDefinition(c.Name))
                        .ToArray(),
                },
            };
        }

        private IEnumerable<IExecutable> CreateTableMainProcess()
        {
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

        private IEnumerable<IExecutable> CreateTableFinalizers()
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