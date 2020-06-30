namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public enum MsSqlDropForeignKeysProcessMode { All, InSpecifiedTables, InSpecifiedSchema, ToSpecifiedSchema, ToSpecifiedTables }

    public class MsSqlDropForeignKeys : AbstractSqlStatements
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropForeignKeysProcessMode.ToSpecifiedTables"/>
        /// </summary>
        public MsSqlDropForeignKeysProcessMode Mode { get; set; } = MsSqlDropForeignKeysProcessMode.ToSpecifiedTables;

        /// <summary>
        /// Table names must include schema name.
        /// </summary>
        public string[] TableNames { get; set; }

        public string SchemaName { get; set; }

        public MsSqlDropForeignKeys(ITopic topic, string name)
            : base(topic, name)
        {
        }

        private List<Tuple<string, int>> _tableNamesAndCounts;

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            switch (Mode)
            {
                case MsSqlDropForeignKeysProcessMode.InSpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new ProcessParameterNullException(this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.InSpecifiedTables));
                    break;
                case MsSqlDropForeignKeysProcessMode.All:
                    if (TableNames != null)
                        throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.All));
                    break;
                case MsSqlDropForeignKeysProcessMode.InSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.InSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new ProcessParameterNullException(this, nameof(SchemaName));
                    break;
                case MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new ProcessParameterNullException(this, nameof(SchemaName));
                    break;
            }

            if (ConnectionString.SqlEngine != SqlEngine.MsSql)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
        {
            var startedOn = Stopwatch.StartNew();
            using (var command = connection.CreateCommand())
            {
                var parameters = new Dictionary<string, object>();

                command.CommandTimeout = CommandTimeout;
                switch (Mode)
                {
                    case MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema:
                        {
                            command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
	inner join sys.objects o on fkc.referenced_object_id = o.object_id and o.schema_id = SCHEMA_ID(@schemaName)";
                            parameters.Add("schemaName", SchemaName);
                            break;
                        }

                    case MsSqlDropForeignKeysProcessMode.InSpecifiedSchema:
                        {
                            command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
where fk.schema_id = SCHEMA_ID(@schemaName)";
                            parameters.Add("schemaName", SchemaName);
                            break;
                        }

                    case MsSqlDropForeignKeysProcessMode.InSpecifiedTables:
                        command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id";
                        break;
                    case MsSqlDropForeignKeysProcessMode.ToSpecifiedTables:
                        command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName, 
	SCHEMA_NAME(o.schema_id) refSchemaName,
	o.name refTableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
	inner join sys.objects o on fkc.referenced_object_id = o.object_id";
                        break;
                }

                // tables are not filtered with an IN clause due to the limitations of the query processor
                // this solution will read unnecessary data, but it will work in all conditions

                command.FillCommandParameters(parameters);

                var tablesNamesHashSet = Mode == MsSqlDropForeignKeysProcessMode.InSpecifiedTables || Mode == MsSqlDropForeignKeysProcessMode.ToSpecifiedTables
                    ? TableNames.Select(x => x.ToUpperInvariant()).ToHashSet()
                    : null;

                var constraintsByTable = new Dictionary<string, List<string>>();

                var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadMeta, ConnectionString.Name, "SYS.FOREIGN_KEYS", command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                    "querying foreign key names from {ConnectionStringName}",
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
                            var sourceTableNameUpper = sourceTableName.ToUpperInvariant();

                            if (Mode == MsSqlDropForeignKeysProcessMode.InSpecifiedTables && !tablesNamesHashSet.Contains(sourceTableNameUpper))
                                continue;

                            if (Mode == MsSqlDropForeignKeysProcessMode.ToSpecifiedTables)
                            {
                                var referredTableName = ConnectionString.Escape((string)reader["refTableName"], (string)reader["refSchemaName"]);
                                if (!tablesNamesHashSet.Contains(sourceTableNameUpper))
                                    continue;
                            }

                            if (!constraintsByTable.TryGetValue(sourceTableName, out var list))
                            {
                                list = new List<string>();
                                constraintsByTable.Add(sourceTableName, list);
                            }

                            list.Add(ConnectionString.Escape((string)reader["fkName"]));
                        }

                        Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadMeta, iocUid, recordsRead);
                    }

                    _tableNamesAndCounts = new List<Tuple<string, int>>();
                    var statements = new List<string>();
                    foreach (var kvp in constraintsByTable.OrderBy(x => x.Key))
                    {
                        _tableNamesAndCounts.Add(new Tuple<string, int>(kvp.Key, kvp.Value.Count));

                        statements.Add("ALTER TABLE " + kvp.Key + " DROP CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
                    }

                    Context.Log(LogSeverity.Debug, this, "{ForeignKeyCount} foreign keys acquired from information schema of {ConnectionStringName} in {Elapsed} for {TableCount} tables",
                        constraintsByTable.Sum(x => x.Value.Count), ConnectionString.Name, startedOn.Elapsed, _tableNamesAndCounts.Count);

                    return statements;
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadMeta, iocUid, null, ex);

                    var exception = new ProcessExecutionException(this, "failed to query foreign key names from information schema", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "foreign key list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                        ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                    exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                    exception.Data.Add("Statement", command.CommandText);
                    exception.Data.Add("Timeout", command.CommandTimeout);
                    exception.Data.Add("Elapsed", startedOn.Elapsed);
                    throw exception;
                }
            }
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var tableName = _tableNamesAndCounts[statementIndex].Item1;
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
                "drop foreign keys of {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(tableName));

            try
            {
                command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "failed to drop foreign keys", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop foreign keys, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(tableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
        {
            if (lastSucceededIndex == -1)
                return;

            var fkCount = _tableNamesAndCounts
                    .Take(lastSucceededIndex + 1)
                    .Sum(x => x.Item2);

            Context.Log(transactionId, LogSeverity.Debug, this, "{ForeignKeyCount} foreign keys for {TableCount} table(s) successfully dropped on {ConnectionStringName} in {Elapsed}",
                fkCount, lastSucceededIndex + 1, ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}