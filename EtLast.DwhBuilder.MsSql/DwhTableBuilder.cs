namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public delegate IEnumerable<IMutator> MutatorCreatorDelegate(DwhTableBuilder tableBuilder);

    [DebuggerDisplay("{Table}")]
    public class DwhTableBuilder : IDwhTableBuilder
    {
        public DwhBuilder DwhBuilder { get; }
        public ResilientTable ResilientTable { get; }
        public RelationalTable Table { get; }

        public string EtlRunInsertColumnNameEscaped { get; }
        public string EtlRunUpdateColumnNameEscaped { get; }
        public string EtlRunFromColumnNameEscaped { get; }
        public string EtlRunToColumnNameEscaped { get; }

        public RelationalColumn ValidFromColumn { get; }
        public string ValidFromColumnNameEscaped { get; }

        public string ValidToColumnName { get; }
        public string ValidToColumnNameEscaped { get; }

        private readonly List<Func<DwhTableBuilder, IEnumerable<IExecutable>>> _finalizerCreators = new List<Func<DwhTableBuilder, IEnumerable<IExecutable>>>();
        private readonly List<MutatorCreatorDelegate> _mutatorCreators = new List<MutatorCreatorDelegate>();
        private Func<IEvaluable> _inputProcessCreator;

        public DwhTableBuilder(DwhBuilder builder, ResilientTable resilientTable, RelationalTable table)
        {
            DwhBuilder = builder;
            ResilientTable = resilientTable;
            Table = table;

            EtlRunInsertColumnNameEscaped = Table[builder.Configuration.EtlRunInsertColumnName]?.NameEscaped(builder.ConnectionString);
            EtlRunUpdateColumnNameEscaped = Table[builder.Configuration.EtlRunUpdateColumnName]?.NameEscaped(builder.ConnectionString);
            EtlRunFromColumnNameEscaped = Table[builder.Configuration.EtlRunFromColumnName]?.NameEscaped(builder.ConnectionString);
            EtlRunToColumnNameEscaped = Table[builder.Configuration.EtlRunToColumnName]?.NameEscaped(builder.ConnectionString);

            ValidFromColumn = Table[builder.Configuration.ValidFromColumnName];
            ValidFromColumnNameEscaped = ValidFromColumn?.NameEscaped(builder.ConnectionString);

            ValidToColumnName = ValidFromColumn != null ? builder.Configuration.ValidToColumnName : null;
            ValidToColumnNameEscaped = ValidToColumnName != null ? builder.ConnectionString.Escape(ValidToColumnName) : null;
        }

        internal void AddMutatorCreator(MutatorCreatorDelegate creator)
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
            ResilientTable.FinalizerCreator = _ => CreateTableFinalizers();
            ResilientTable.MainProcessCreator = t => CreateTableMainProcess();
        }

        private IMutator CreateTempWriter(ResilientTable table, RelationalTable dwhTable)
        {
            var tempColumns = dwhTable.Columns
                .Where(x => !x.GetUsedByEtlRunInfo());

            if (dwhTable.AnyPrimaryKeyColumnIsIdentity)
            {
                tempColumns = tempColumns
                    .Where(x => !x.IsPrimaryKey);
            }

            return new MsSqlWriteToTableWithMicroTransactionsMutator(table.Topic, "Writer")
            {
                ConnectionString = table.Scope.Configuration.ConnectionString,
                TableDefinition = new DbTableDefinition()
                {
                    TableName = table.TempTableName,
                    Columns = tempColumns
                        .Select(c => new DbColumnDefinition(c.Name, c.NameEscaped(DwhBuilder.ConnectionString)))
                        .ToArray(),
                },
            };
        }

        private IEnumerable<IExecutable> CreateTableMainProcess()
        {
            var mutators = new MutatorList();
            foreach (var creator in _mutatorCreators)
            {
                mutators.Add(creator?.Invoke(this));
            }

            mutators.Add(CreateTempWriter(ResilientTable, Table));

            var inputProcess = _inputProcessCreator?.Invoke();

            yield return new ProcessBuilder()
            {
                InputProcess = inputProcess,
                Mutators = mutators,
            }.Build();
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