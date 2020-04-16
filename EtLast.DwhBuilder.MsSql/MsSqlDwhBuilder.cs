namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public class MsSqlDwhBuilder : IDwhBuilder<DwhTableBuilder>
    {
        public ITopic Topic { get; }
        public string ScopeName { get; }

        public RelationalModel Model { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public IReadOnlyList<SqlEngineVersion> SupportedSqlEngineVersions { get; } = new List<SqlEngineVersion> { MsSqlVersion.MsSql2016 };

        private DwhBuilderConfiguration _configuration;
        public DwhBuilderConfiguration Configuration { get => _configuration; set => SetConfiguration(value); }

        public IEnumerable<RelationalTable> Tables => _tables.Select(x => x.Table);
        private readonly List<DwhTableBuilder> _tables = new List<DwhTableBuilder>();

        internal DateTimeOffset? DefaultValidFromDateTime => Configuration.UseEtlRunIdTimeForDefaultValidFrom
            ? EtlRunId
            : Configuration.InfinitePastDateTime;

        private readonly List<ResilientSqlScopeExecutableCreatorDelegate> _postFinalizerCreators = new List<ResilientSqlScopeExecutableCreatorDelegate>();
        private readonly DateTime? _etlRunIdUtcOverride;

        public DateTime? EtlRunId { get; private set; }
        public DateTimeOffset? EtlRunIdAsDateTimeOffset { get; private set; }

        public MsSqlDwhBuilder(ITopic topic, string scopeName, DateTime? etlRunIdUtcOverride = null)
        {
            Topic = topic;
            ScopeName = scopeName;
            _etlRunIdUtcOverride = etlRunIdUtcOverride;
        }

        private void SetConfiguration(DwhBuilderConfiguration configuration)
        {
            _tables.Clear();
            _configuration = configuration;
        }

        public ResilientSqlScope Build()
        {
            if (Configuration == null)
                throw new DwhBuilderParameterNullException<DwhTableBuilder>(this, nameof(Configuration));

            if (ConnectionString == null)
                throw new DwhBuilderParameterNullException<DwhTableBuilder>(this, nameof(ConnectionString));

            foreach (var tableBuilder in _tables)
            {
                tableBuilder.Build();
            }

            return new ResilientSqlScope(Topic, ScopeName)
            {
                Configuration = new ResilientSqlScopeConfiguration()
                {
                    ConnectionString = ConnectionString,
                    TempTableMode = Configuration.TempTableMode,
                    Tables = _tables.Select(x => x.ResilientTable).ToList(),
                    InitializerCreator = CreateInitializers,
                    FinalizerRetryCount = Configuration.FinalizerRetryCount,
                    FinalizerTransactionScopeKind = TransactionScopeKind.RequiresNew,
                    PostFinalizerCreator = CreatePostFinalizers,
                },
            };
        }

        private IEnumerable<IExecutable> CreatePostFinalizers(ResilientSqlScope scope, IProcess caller)
        {
            // todo: this should be built and configured by DisableConstraintCheck
            var constraintCheckDisabledOnTables = scope.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
            if (constraintCheckDisabledOnTables != null)
            {
                yield return new MsSqlEnableConstraintCheck(scope.Topic, "EnableConstraintCheck")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    TableNames = constraintCheckDisabledOnTables.Distinct().OrderBy(x => x).ToArray(),
                    CommandTimeout = 60 * 60,
                };
            }

            var etlRunInfoTable = Model.GetEtlRunInfoTable();
            if (etlRunInfoTable != null)
            {
                yield return new CustomSqlStatement(scope.Topic.Child(etlRunInfoTable.SchemaAndName), "UpdateEtlRun")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    CommandTimeout = 60 * 60,
                    MainTableName = etlRunInfoTable.EscapedName(ConnectionString),
                    SqlStatement = "UPDATE " + etlRunInfoTable.EscapedName(ConnectionString)
                        + " SET FinishedOn = @FinishedOn, Result = @Result"
                        + " WHERE StartedOn = @EtlRunId",
                    Parameters = new Dictionary<string, object>
                    {
                        ["FinishedOn"] = DateTime.UtcNow,
                        ["Result"] = "success",
                        ["EtlRunid"] = EtlRunId.Value,
                    },
                };
            }

            foreach (var creator in _postFinalizerCreators)
            {
                var result = creator.Invoke(scope, caller);
                if (result != null)
                {
                    foreach (var process in result)
                        yield return process;
                }
            }
        }

        internal string GetEscapedTempTableName(RelationalTable dwhTable)
        {
            return ConnectionString.Escape(Configuration.TempTableNamePrefix + dwhTable.Name, dwhTable.Schema.Name);
        }

        internal string GetEscapedHistTableName(RelationalTable dwhTable)
        {
            if (!dwhTable.GetHasHistoryTable())
                return null;

            return ConnectionString.Escape(dwhTable.Name + Configuration.HistoryTableNamePostfix, dwhTable.Schema.Name);
        }

        private IEnumerable<IExecutable> CreateInitializers(ResilientSqlScope scope, IProcess caller)
        {
            var etlRunInfoTable = Model.GetEtlRunInfoTable();
            if (etlRunInfoTable != null)
            {
                yield return new ProcessBuilder()
                {
                    InputProcess = new EnumerableImporter(scope.Topic.Child(etlRunInfoTable.SchemaAndName), "RowCreator")
                    {
                        InputGenerator = process =>
                        {
                            var currentId = _etlRunIdUtcOverride ?? DateTime.UtcNow;

                            currentId = new DateTime(
                                currentId.Year,
                                currentId.Month,
                                currentId.Day,
                                currentId.Hour,
                                currentId.Minute,
                                currentId.Second,
                                0,
                                DateTimeKind.Utc);

                            EtlRunId = currentId;
                            EtlRunIdAsDateTimeOffset = new DateTimeOffset(currentId, new TimeSpan(0));

                            scope.Topic.Context.AdditionalData["CurrentEtlRunId"] = currentId;

                            var row = new SlimRow()
                            {
                                ["StartedOn"] = currentId,
                                ["Name"] = scope.Name,
                                ["MachineName"] = Environment.MachineName,
                                ["UserName"] = Environment.UserName,
                            };

                            return new[] { row };
                        }
                    },
                    Mutators = new MutatorList()
                    {
                        new MsSqlWriteToTableWithMicroTransactionsMutator(scope.Topic.Child(etlRunInfoTable.SchemaAndName), "Writer")
                        {
                            ConnectionString = ConnectionString,
                            TableDefinition = new DbTableDefinition()
                            {
                                TableName = etlRunInfoTable.EscapedName(ConnectionString),
                                Columns = new[] { "StartedOn", "Name", "MachineName", "UserName" }
                                    .Select(c => new DbColumnDefinition(c, ConnectionString.Escape(c)))
                                    .ToArray(),
                            },
                        },
                    },
                }.Build();
            }
        }

        public DwhTableBuilder[] AddTables(params RelationalTable[] tables)
        {
            var result = new DwhTableBuilder[tables.Length];

            for (var i = 0; i < tables.Length; i++)
            {
                var table = tables[i];

                var tempColumns = table.Columns
                    .Where(x => !x.GetUsedByEtlRunInfo());

                if (table.AnyPrimaryKeyColumnIsIdentity)
                {
                    tempColumns = tempColumns
                        .Where(x => !x.IsPrimaryKey);
                }

                var resilientTable = new ResilientTable()
                {
                    TableName = table.EscapedName(ConnectionString),
                    TempTableName = GetEscapedTempTableName(table),
                    Columns = tempColumns.Select(x => x.Name).ToArray(),
                };

                var tableBuilder = new DwhTableBuilder(this, resilientTable, table);

                _tables.Add(tableBuilder);
                result[i] = tableBuilder;
            }

            return result;
        }

        public void AddPostFinalizer(ResilientSqlScopeExecutableCreatorDelegate creator)
        {
            _postFinalizerCreators.Add(creator);
        }
    }
}