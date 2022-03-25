namespace FizzCode.EtLast.DwhBuilder.MsSql;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using FizzCode.EtLast;
using FizzCode.LightWeight.AdoNet;
using FizzCode.LightWeight.RelationalModel;

public class MsSqlDwhBuilder : IDwhBuilder<DwhTableBuilder>
{
    public IEtlContext Context { get; }
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

    private readonly List<Action<ResilientSqlScopeProcessBuilder>> _preFinalizerCreators = new();
    private readonly List<Action<ResilientSqlScopeProcessBuilder>> _postFinalizerCreators = new();
    private readonly DateTime? _etlRunIdUtcOverride;

    public DateTime? EtlRunId { get; private set; }
    public DateTimeOffset? EtlRunIdAsDateTimeOffset { get; private set; }

    private readonly Dictionary<string, List<string>> _enabledConstraintsByTable = new(StringComparer.OrdinalIgnoreCase);

    public MsSqlDwhBuilder(IEtlContext context, string scopeName, DateTime? etlRunIdUtcOverride = null)
    {
        Context = context;
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

        return new ResilientSqlScope(Context)
        {
            Name = ScopeName,
            ConnectionString = ConnectionString,
            TempTableMode = Configuration.TempTableMode,
            Tables = _tables.ConvertAll(x => x.ResilientTable),
            Initializers = CreateInitializers,
            FinalizerRetryCount = Configuration.FinalizerRetryCount,
            FinalizerTransactionScopeKind = TransactionScopeKind.RequiresNew,
            PreFinalizers = CreatePreFinalizers,
            PostFinalizers = CreatePostFinalizers,
        };
    }

    private void CreatePreFinalizers(ResilientSqlScopeProcessBuilder builder)
    {
        var process = new CustomAction(Context)
        {
            Name = "ReadAllEnabledForeignKeys",
            Action = (proc) =>
            {
                var startedOn = Stopwatch.StartNew();
                var connection = EtlConnectionManager.GetNewConnection(ConnectionString, proc);
                using (var command = connection.Connection.CreateCommand())
                {
                    command.CommandTimeout = 60 * 60;
                    command.CommandText = @"
                            select distinct
	                            fk.[name] fkName,
	                            SCHEMA_NAME(fk.schema_id) schemaName,
	                            OBJECT_NAME(fk.parent_object_id) tableName
                            from
	                            sys.foreign_keys fk
                                where fk.is_disabled=0";

                    var iocUid = builder.Scope.Context.RegisterIoCommandStart(proc, IoCommandKind.dbReadMeta, ConnectionString.Name, "SYS.FOREIGN_KEYS", command.CommandTimeout, command.CommandText, null, null,
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

                            builder.Scope.Context.RegisterIoCommandSuccess(proc, IoCommandKind.dbReadMeta, iocUid, recordsRead);
                        }

                        builder.Scope.Context.Log(LogSeverity.Information, proc, "{ForeignKeyCount} enabled foreign keys acquired from information schema of {ConnectionStringName} in {Elapsed}",
                            _enabledConstraintsByTable.Sum(x => x.Value.Count), ConnectionString.Name, startedOn.Elapsed);
                    }
                    catch (Exception ex)
                    {
                        builder.Scope.Context.RegisterIoCommandFailed(proc, IoCommandKind.dbReadMeta, iocUid, null, ex);

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

        builder.Processes.Add(process);

        foreach (var creator in _preFinalizerCreators)
        {
            creator.Invoke(builder);
        }
    }

    private void CreatePostFinalizers(ResilientSqlScopeProcessBuilder builder)
    {
        // todo: this should be built and configured by DisableConstraintCheck
        var constraintCheckDisabledOnTables = builder.Scope.Context.AdditionalData.GetAs<List<string>>("ConstraintCheckDisabledOnTables", null);
        if (constraintCheckDisabledOnTables != null)
        {
            builder.Processes.Add(new MsSqlEnableConstraintCheckFiltered(builder.Scope.Context)
            {
                Name = "EnableForeignKeys",
                ConnectionString = builder.Scope.ConnectionString,
                ConstraintNames = constraintCheckDisabledOnTables
                    .Distinct()
                    .Where(x => _enabledConstraintsByTable.ContainsKey(x))
                    .OrderBy(x => x)
                    .Select(x => new KeyValuePair<string, List<string>>(x, _enabledConstraintsByTable[x]))
                    .ToList(),
                CommandTimeout = 60 * 60,
            });
        }

        var etlRunInfoTable = Model.GetEtlRunInfoTable();
        if (etlRunInfoTable != null)
        {
            builder.Processes.Add(new CustomSqlStatement(builder.Scope.Context)
            {
                Name = "UpdateEtlRun",
                ConnectionString = builder.Scope.ConnectionString,
                CommandTimeout = 60 * 60,
                MainTableName = etlRunInfoTable.SchemaAndName,
                SqlStatement = "UPDATE " + etlRunInfoTable.EscapedName(ConnectionString)
                    + " SET FinishedOn = @FinishedOn, Result = @Result"
                    + " WHERE StartedOn = @EtlRunId",
                Parameters = new Dictionary<string, object>
                {
                    ["FinishedOn"] = DateTime.UtcNow,
                    ["Result"] = "success",
                    ["EtlRunid"] = EtlRunId.Value,
                },
            });
        }

        foreach (var creator in _postFinalizerCreators)
        {
            creator.Invoke(builder);
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

    private void CreateInitializers(ResilientSqlScopeProcessBuilder builder)
    {
        var etlRunInfoTable = Model.GetEtlRunInfoTable();
        if (etlRunInfoTable != null)
        {
            var process = new ProcessBuilder()
            {
                InputProcess = new EnumerableImporter(builder.Scope.Context)
                {
                    Name = "EtlRunInfoCreator",
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

                        builder.Scope.Context.AdditionalData["CurrentEtlRunId"] = currentId;

                        var row = new SlimRow()
                        {
                            ["StartedOn"] = currentId,
                            ["Name"] = builder.Scope.Name,
                            ["MachineName"] = Environment.MachineName,
                            ["UserName"] = Environment.UserName,
                        };

                        return new[] { row };
                    }
                },
                Mutators = new MutatorList()
                {
                    new ResilientWriteToMsSqlMutator(builder.Scope.Context)
                    {
                        Name = "EtlRunInfoWriter",
                        ConnectionString = ConnectionString,
                        TableDefinition = new DbTableDefinition()
                        {
                            TableName = etlRunInfoTable.EscapedName(ConnectionString),
                            Columns = new()
                            {
                                ["StartedOn"] = ConnectionString.Escape("StartedOn"),
                                ["Name"] = ConnectionString.Escape("Name"),
                                ["MachineName"] = ConnectionString.Escape("MachineName"),
                                ["UserName"] = ConnectionString.Escape("UserName"),
                            },
                        },
                    },
                },
            }.Build();

            builder.Processes.Add(process);
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

    public void AddPreFinalizer(Action<ResilientSqlScopeProcessBuilder> finalizers)
    {
        _preFinalizerCreators.Add(finalizers);
    }

    public void AddPostFinalizer(Action<ResilientSqlScopeProcessBuilder> finalizers)
    {
        _postFinalizerCreators.Add(finalizers);
    }
}
