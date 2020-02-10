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
        public string Topic => DwhBuilder.ConnectionString.Unescape(Table.TableName);

        public SqlColumn RecordTimestampIndicatorColumn { get; }
        public string EtlInsertRunIdColumnNameEscaped { get; }
        public string EtlUpdateRunIdColumnNameEscaped { get; }

        public string ValidFromColumnName { get; }
        public string ValidToColumnName { get; }
        public string ValidFromColumnNameEscaped { get; }
        public string ValidToColumnNameEscaped { get; }

        private readonly List<Func<DwhTableBuilder, IEnumerable<IExecutable>>> _finalizerCreators = new List<Func<DwhTableBuilder, IEnumerable<IExecutable>>>();
        private readonly List<Func<DwhTableBuilder, IEnumerable<IMutator>>> _mutatorCreators = new List<Func<DwhTableBuilder, IEnumerable<IMutator>>>();
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

            ValidToColumnName = ValidFromColumnName != null ? builder.Configuration.ValidToColumnName : null;
            ValidToColumnNameEscaped = ValidToColumnName != null ? builder.ConnectionString.Escape(ValidToColumnName) : null;
        }

        internal void AddMutatorCreator(Func<DwhTableBuilder, IEnumerable<IMutator>> creator)
        {
            _mutatorCreators.Add(creator);
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

        private IMutator CreateTempWriter(ResilientTable table, SqlTable sqlTable)
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

            return new MsSqlWriteToTableWithMicroTransactionsMutator(DwhBuilder.Context, "Writer", table.Topic)
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = tempColumns
                        .Select(c => new DbColumnDefinition(c.Name, DwhBuilder.ConnectionString.Escape(c.Name)))
                        .ToArray(),
                },
            };
        }

        private IEnumerable<IExecutable> CreateTableMainProcess()
        {
            var mutators = new List<IMutator>();
            foreach (var creator in _mutatorCreators)
            {
                mutators.AddRange(creator?.Invoke(this));
            }

            mutators.Add(CreateTempWriter(Table, SqlTable));

            var inputProcess = _inputProcessCreator?.Invoke();

            yield return new MutatorBuilder()
            {
                InputProcess = inputProcess,
                Mutators = mutators,
            }.BuildEvaluable();
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