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

        private List<string> _tableNames;

        protected override void Validate(IProcess process)
        {
            // no base.Validate call

            switch (Mode)
            {
                case MsSqlDropForeignKeysJobMode.InSpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new JobParameterNullException(process, this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.InSpecifiedTables));
                    break;
                case MsSqlDropForeignKeysJobMode.All:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    break;
                case MsSqlDropForeignKeysJobMode.InSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.InSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new JobParameterNullException(process, this, nameof(SchemaName));
                    break;
                case MsSqlDropForeignKeysJobMode.ToSpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.ToSpecifiedSchema));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new JobParameterNullException(process, this, nameof(SchemaName));
                    break;
            }

            var knownProvider = process.Context.GetConnectionString(ConnectionStringKey)?.KnownProvider;
            if (knownProvider != KnownProvider.MsSql)
                throw new InvalidJobParameterException(process, this, nameof(ConnectionString), nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(IProcess process, ConnectionStringWithProvider connectionString, IDbConnection connection)
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

                    process.Context.Log(LogSeverity.Debug, process, "({Job}) querying foreign key names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                        Name, ConnectionString.Name, command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

                    var tablesNamesHashSet = Mode == MsSqlDropForeignKeysJobMode.InSpecifiedTables || Mode == MsSqlDropForeignKeysJobMode.ToSpecifiedTables
                        ? TableNames.Select(x => x.ToLowerInvariant()).ToHashSet()
                        : null;

                    var constraintsByTable = new Dictionary<string, List<string>>();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var sourceTableName = "[" + (string)reader["schemaName"] + "].[" + (string)reader["tableName"] + "]";

                            if (Mode == MsSqlDropForeignKeysJobMode.InSpecifiedTables && !tablesNamesHashSet.Contains(sourceTableName))
                                continue;

                            if (Mode == MsSqlDropForeignKeysJobMode.ToSpecifiedTables)
                            {
                                var referredTableName = "[" + (string)reader["refSchemaName"] + "].[" + (string)reader["refTableName"] + "]";
                                if (!tablesNamesHashSet.Contains(sourceTableName))
                                    continue;
                            }

                            if (!constraintsByTable.TryGetValue(sourceTableName, out var list))
                            {
                                list = new List<string>();
                                constraintsByTable.Add(sourceTableName, list);
                            }

                            list.Add("[" + (string)reader["fkName"] + "]");
                        }
                    }

                    _tableNames = new List<string>();
                    var statements = new List<string>();
                    foreach (var kvp in constraintsByTable.OrderBy(x => x.Key))
                    {
                        _tableNames.Add(kvp.Key);

                        statements.Add("ALTER TABLE " + kvp.Key + " DROP CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
                    }

                    var modeInfo = Mode switch
                    {
                        MsSqlDropForeignKeysJobMode.All => " (all foreign keys in database)",
                        MsSqlDropForeignKeysJobMode.InSpecifiedSchema => " (in schema '" + SchemaName + "')",
                        MsSqlDropForeignKeysJobMode.ToSpecifiedSchema => " (to schema '" + SchemaName + "')",
                        MsSqlDropForeignKeysJobMode.InSpecifiedTables => " (in tables '" + string.Join(",", TableNames) + "')",
                        MsSqlDropForeignKeysJobMode.ToSpecifiedTables => " (to tables '" + string.Join(",", TableNames) + "')",
                        _ => null,
                    };

                    process.Context.Log(LogSeverity.Information, process, "{ForeignKeyCount} foreign keys aquired from information schema in {Elapsed} for {TableCount} tables" + modeInfo,
                        constraintsByTable.Sum(x => x.Value.Count), startedOn.Elapsed, _tableNames.Count);

                    return statements;
                }
                catch (Exception ex)
                {
                    var exception = new JobExecutionException(process, this, "failed to query foreign key names from information schema", ex);
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

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = _tableNames[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "({Job}) drop foreign keys of {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Debug, process, "({Job}) foreign keys on {ConnectionStringKey}/{TableName} are dropped in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to drop foreign keys", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop foreign keys, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeViewName(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableNameName", Helpers.UnEscapeViewName(tableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(IProcess process, int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            process.Context.Log(LogSeverity.Information, process, "({Job}) all foreign keys for {TableCount} table(s) successfully dropped on {ConnectionStringKey} in {Elapsed}: {TableNames}",
                 Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed,
                 _tableNames
                    .Take(lastSucceededIndex + 1)
                    .Select(Helpers.UnEscapeTableName)
                    .ToArray());
        }
    }
}