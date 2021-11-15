namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public class MsSqlDwhBuilder : IDwhBuilder<DwhTableBuilder>
    {
        public IEtlContext Context { get; }
        public string Topic { get; }
        public string ScopeName { get; }

        public RelationalModel Model { get; init; }
        public NamedConnectionString ConnectionString { get; init; }
        public IReadOnlyList<SqlEngine> SupportedSqlEngines { get; } = new List<SqlEngine> { SqlEngine.MsSql };

        public DwhBuilderConfiguration Configuration { get; init; }

        public IEnumerable<RelationalTable> Tables => _tables.Select(x => x.Table);
        private readonly List<DwhTableBuilder> _tables = new();

        internal DateTimeOffset? DefaultValidFromDateTime => Configuration.UseEtlRunIdForDefaultValidFrom
            ? EtlRunId
            : Configuration.InfinitePastDateTime;

        private readonly List<ResilientSqlScopeExecutableCreatorDelegate> _preFinalizerCreators = new();
        private readonly List<ResilientSqlScopeExecutableCreatorDelegate> _postFinalizerCreators = new();
        private readonly DateTime? _etlRunIdUtcOverride;

        public DateTime? EtlRunId { get; private set; }
        public DateTimeOffset? EtlRunIdAsDateTimeOffset { get; private set; }

        private readonly Dictionary<string, List<string>> _enabledConstraintsByTable = new(StringComparer.OrdinalIgnoreCase);

        public MsSqlDwhBuilder(IEtlContext context, string topic, string scopeName, DateTime? etlRunIdUtcOverride = null)
        {
            Context = context;
            Topic = topic;
            ScopeName = scopeName;
            _etlRunIdUtcOverride = etlRunIdUtcOverride;
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

            return new ResilientSqlScope(Context, Topic, ScopeName)
            {
                Configuration = new ResilientSqlScopeConfiguration()
                {
                    ConnectionString = ConnectionString,
                    TempTableMode = Configuration.TempTableMode,
                    Tables = _tables.ConvertAll(x => x.ResilientTable),
                    InitializerCreator = CreateInitializers,
                    FinalizerRetryCount = Configuration.FinalizerRetryCount,
                    FinalizerTransactionScopeKind = TransactionScopeKind.RequiresNew,
                    PreFinalizerCreator = CreatePreFinalizers,
                    PostFinalizerCreator = CreatePostFinalizers,
                },
            };
        }

        private IEnumerable<IExecutable> CreatePreFinalizers(ResilientSqlScope scope)
        {
            yield return new CustomAction(scope.Context, scope.Topic, "ReadAllEnabledForeignKeys")
            {
                Then = (proc) =>
                {
                    var startedOn = Stopwatch.StartNew();
                    var connection = EtlConnectionManager.GetNewConnection(ConnectionString, proc);
                    using (var command = connection.Connection.CreateCommand())
                    {
                        command.CommandTimeout = 60 * 1000;
                        command.CommandText = @"
                            select distinct
	                            fk.[name] fkName,
	                            SCHEMA_NAME(fk.schema_id) schemaName,
	                            OBJECT_NAME(fk.parent_object_id) tableName
                            from
	                            sys.foreign_keys fk
                                where fk.is_disabled=0";

                        var iocUid = scope.Context.RegisterIoCommandStart(proc, IoCommandKind.dbReadMeta, ConnectionString.Name, "SYS.FOREIGN_KEYS", command.CommandTimeout, command.CommandText, null, null,
                            "querying enabled foreign key names from {ConnectionStringName}",
                            ConnectionString.Name);

                        var recordsRead = 0;
                        try
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    recordsRead++;
                                    var sourceTableName = ConnectionString.Escape((string)reader["tableName"], (string)reader["schemaName"]);

                                    if (!_enabledConstraintsByTable.TryGetValue(sourceTableName, out var list))
                                    {
                                        list = new List<string>();
                                        _enabledConstraintsByTable.Add(sourceTableName, list);
                                    }

                                    list.Add(ConnectionString.Escape((string)reader["fkName"]));
                                }

                                scope.Context.RegisterIoCommandSuccess(proc, IoCommandKind.dbReadMeta, iocUid, recordsRead);
                            }

                            scope.Context.Log(LogSeverity.Information, proc, "{ForeignKeyCount} enabled foreign keys acquired from information schema of {ConnectionStringName} in {Elapsed}",
                                _enabledConstraintsByTable.Sum(x => x.Value.Count), ConnectionString.Name, startedOn.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            scope.Context.RegisterIoCommandFailed(proc, IoCommandKind.dbReadMeta, iocUid, null, ex);

                            var exception = new SqlSchemaReadException(proc, "enabled foreign key names", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "enabled foreign key list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                            exception.Data.Add("Statement", command.CommandText);
                            exception.Data.Add("Timeout", command.CommandTimeout);
                            exception.Data.Add("Elapsed", startedOn.Elapsed);
                            throw exception;
                        }
                    }

                    EtlConnectionManager.ReleaseConnection(proc, ref connection);
                }
            };

            foreach (var creator in _preFinalizerCreators)
            {
                var result = creator.Invoke(scope);
                if (result != null)
                {
                    foreach (var process in result)
                        yield return process;
                }
            }
        }

        private IEnumerable<IExecutable> CreatePostFinalizers(ResilientSqlScope scope)
        {
            // todo: this should be built and configured by DisableConstraintCheck
            var constraintCheckDisabledOnTables = scope.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
            if (constraintCheckDisabledOnTables != null)
            {
                yield return new MsSqlEnableConstraintCheckFiltered(scope.Context, scope.Topic, "EnableForeignKeys")
                {
                    ConnectionString = scope.Configuration.ConnectionString,
                    ConstraintNames = constraintCheckDisabledOnTables
                        .Distinct()
                        .Where(x => _enabledConstraintsByTable.ContainsKey(x))
                        .OrderBy(x => x)
                        .Select(x => new KeyValuePair<string, List<string>>(x, _enabledConstraintsByTable[x]))
                        .ToList(),
                    CommandTimeout = 60 * 60,
                };
            }

            var etlRunInfoTable = Model.GetEtlRunInfoTable();
            if (etlRunInfoTable != null)
            {
                yield return new CustomSqlStatement(scope.Context, etlRunInfoTable.SchemaAndName, "UpdateEtlRun")
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
                var result = creator.Invoke(scope);
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

        private IEnumerable<IExecutable> CreateInitializers(ResilientSqlScope scope)
        {
            var etlRunInfoTable = Model.GetEtlRunInfoTable();
            if (etlRunInfoTable != null)
            {
                yield return new ProcessBuilder()
                {
                    InputProcess = new EnumerableImporter(scope.Context, etlRunInfoTable.SchemaAndName, "RowCreator")
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
                                currentId.Millisecond / 10 * 10, // 10 msec accuracy
                                DateTimeKind.Utc);

                            EtlRunId = currentId;
                            EtlRunIdAsDateTimeOffset = new DateTimeOffset(currentId, new TimeSpan(0));

                            scope.Context.AdditionalData["CurrentEtlRunId"] = currentId;

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
                        new ResilientWriteToMsSqlMutator(scope.Context, etlRunInfoTable.SchemaAndName, "EtlRunInfoWriter")
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

        public void AddPreFinalizer(ResilientSqlScopeExecutableCreatorDelegate creator)
        {
            _preFinalizerCreators.Add(creator);
        }

        public void AddPostFinalizer(ResilientSqlScopeExecutableCreatorDelegate creator)
        {
            _postFinalizerCreators.Add(creator);
        }
    }
}