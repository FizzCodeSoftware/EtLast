namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public enum MsSqlDropForeignKeysJobMode { All, InSpecifiedTables, InSpecifiedSchema, ToSpecifiedSchema, ToSpecifiedTables }

    public class MsSqlDropForeignKeysJob : AbstractSqlStatementsJob
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropForeignKeysJobMode.ToSpecifiedTables"/>
        /// </summary>
        public MsSqlDropForeignKeysJobMode Mode { get; set; } = MsSqlDropForeignKeysJobMode.ToSpecifiedTables;

        /// <summary>
        /// Table names must include schema name.
        /// </summary>
        public string[] TableNames { get; set; }

        public string SchemaName { get; set; }

        private List<Tuple<string, int>> _tableNamesAndCounts;

        protected override void Validate()
        {
            // no base.Validate call

            switch (Mode)
            {
                case MsSqlDropForeignKeysJobMode.InSpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new JobParameterNullException(Process, this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(Process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.InSpecifiedTables));
                    break;
                case MsSqlDropForeignKeysJobMode.All:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(Process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(Process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    break;
                case MsSqlDropForeignKeysJobMode.InSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(Process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.InSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new JobParameterNullException(Process, this, nameof(SchemaName));
                    break;
                case MsSqlDropForeignKeysJobMode.ToSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(Process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.ToSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new JobParameterNullException(Process, this, nameof(SchemaName));
                    break;
            }

            var knownProvider = Process.Context.GetConnectionString(ConnectionStringKey)?.KnownProvider;
            if (knownProvider != KnownProvider.SqlServer)
                throw new InvalidJobParameterException(Process, this, nameof(ConnectionString), nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            var startedOn = Stopwatch.StartNew();
            using (var command = connection.CreateCommand())
            {
                try
                {
                    command.CommandTimeout = CommandTimeout;
                    switch (Mode)
                    {
                        case MsSqlDropForeignKeysJobMode.ToSpecifiedSchema:
                            {
                                command.CommandText = @"
select
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
	inner join sys.objects o on fkc.referenced_object_id = o.object_id and o.schema_id = SCHEMA_ID(@schemaName)";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "schemaName";
                                parameter.Value = SchemaName;
                                command.Parameters.Add(parameter);
                                break;
                            }

                        case MsSqlDropForeignKeysJobMode.InSpecifiedSchema:
                            {
                                command.CommandText = @"
select
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
where fk.schema_id = SCHEMA_ID(@schemaName)";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "schemaName";
                                parameter.Value = SchemaName;
                                command.Parameters.Add(parameter);
                                break;
                            }

                        case MsSqlDropForeignKeysJobMode.InSpecifiedTables:
                            command.CommandText = @"
select
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id";
                            break;
                        case MsSqlDropForeignKeysJobMode.ToSpecifiedTables:
                            command.CommandText = @"
select
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

                    Process.Context.Log(LogSeverity.Debug, Process, this, null, "querying foreign key names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                        ConnectionString.Name, command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

                    var tablesNamesHashSet = Mode == MsSqlDropForeignKeysJobMode.InSpecifiedTables || Mode == MsSqlDropForeignKeysJobMode.ToSpecifiedTables
                        ? TableNames.Select(x => x.ToLowerInvariant()).ToHashSet()
                        : null;

                    var constraintsByTable = new Dictionary<string, List<string>>();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var sourceTableName = ConnectionString.Escape((string)reader["tableName"], (string)reader["schemaName"]);

                            if (Mode == MsSqlDropForeignKeysJobMode.InSpecifiedTables && !tablesNamesHashSet.Contains(sourceTableName))
                                continue;

                            if (Mode == MsSqlDropForeignKeysJobMode.ToSpecifiedTables)
                            {
                                var referredTableName = ConnectionString.Escape((string)reader["refTableName"], (string)reader["refSchemaName"]);
                                if (!tablesNamesHashSet.Contains(sourceTableName))
                                    continue;
                            }

                            if (!constraintsByTable.TryGetValue(sourceTableName, out var list))
                            {
                                list = new List<string>();
                                constraintsByTable.Add(sourceTableName, list);
                            }

                            list.Add(ConnectionString.Escape((string)reader["fkName"]));
                        }
                    }

                    _tableNamesAndCounts = new List<Tuple<string, int>>();
                    var statements = new List<string>();
                    foreach (var kvp in constraintsByTable.OrderBy(x => x.Key))
                    {
                        _tableNamesAndCounts.Add(new Tuple<string, int>(kvp.Key, kvp.Value.Count));

                        statements.Add("ALTER TABLE " + kvp.Key + " DROP CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
                    }

                    Process.Context.Log(LogSeverity.Information, Process, "{ForeignKeyCount} foreign keys aquired from information schema on {ConnectionStringKey} in {Elapsed} for {TableCount} tables",
                        constraintsByTable.Sum(x => x.Value.Count), ConnectionString.Name, startedOn.Elapsed, _tableNamesAndCounts.Count);

                    return statements;
                }
                catch (Exception ex)
                {
                    var exception = new JobExecutionException(Process, this, "failed to query foreign key names from information schema", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "foreign key list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                        ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                    exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                    exception.Data.Add("Statement", command.CommandText);
                    exception.Data.Add("Timeout", command.CommandTimeout);
                    exception.Data.Add("Elapsed", startedOn.Elapsed);
                    throw exception;
                }
            }
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var t = _tableNamesAndCounts[statementIndex];

            Process.Context.Log(LogSeverity.Debug, Process, this, null, "drop foreign keys of {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, ConnectionString.Unescape(t.Item1), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();
                Process.Context.Log(LogSeverity.Debug, Process, this, null, "foreign keys on {ConnectionStringKey}/{TableName} are dropped in {Elapsed}, transaction: {Transaction}",
                    ConnectionString.Name, ConnectionString.Unescape(t.Item1), startedOn.Elapsed, Transaction.Current.ToIdentifierString());

                Process.Context.Stat.IncrementCounter("database foreign keys dropped / " + ConnectionString.Name, t.Item2);
                Process.Context.Stat.IncrementCounter("database foreign keys time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "failed to drop foreign keys", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop foreign keys, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(t.Item1), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(t.Item1));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            var fkCount = _tableNamesAndCounts
                    .Take(lastSucceededIndex + 1)
                    .Sum(x => x.Item2);

            Process.Context.Log(LogSeverity.Information, Process, this, null, "{ForeignKeyCount} foreign keys for {TableCount} table(s) successfully dropped on {ConnectionStringKey} in {Elapsed}, transaction: {Transaction}",
                 fkCount, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}