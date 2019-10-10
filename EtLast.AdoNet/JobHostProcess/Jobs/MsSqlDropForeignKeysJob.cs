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

    public enum MsSqlDropForeignKeysJobMode { All, SpecifiedTables, SpecifiedSchema }

    public class MsSqlDropForeignKeysJob : AbstractSqlStatementsJob
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropForeignKeysJobMode.SpecifiedTables"/>
        /// </summary>
        public MsSqlDropForeignKeysJobMode Mode { get; set; } = MsSqlDropForeignKeysJobMode.SpecifiedTables;

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
                case MsSqlDropForeignKeysJobMode.SpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new JobParameterNullException(process, this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.SpecifiedTables));
                    break;
                case MsSqlDropForeignKeysJobMode.All:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    break;
                case MsSqlDropForeignKeysJobMode.SpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysJobMode.All));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new JobParameterNullException(process, this, nameof(SchemaName));
                    break;
            }

            var providerName = process.Context.GetConnectionString(ConnectionStringKey)?.ProviderName;
            if (providerName != "System.Data.SqlClient")
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
                    command.CommandText = "select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_TYPE='FOREIGN KEY'";
                    if (Mode == MsSqlDropForeignKeysJobMode.SpecifiedSchema)
                    {
                        command.CommandText += " and TABLE_SCHEMA = @schemaName";
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = "schemaName";
                        parameter.Value = SchemaName;
                        command.Parameters.Add(parameter);
                    }

                    process.Context.Log(LogSeverity.Debug, process, "({Job}) querying foreign key names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                        Name, ConnectionString.Name, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");

                    var tablesNamesHashSet = Mode == MsSqlDropForeignKeysJobMode.SpecifiedTables
                        ? TableNames.Select(x => x.ToLowerInvariant()).ToHashSet()
                        : null;

                    var tableConstraintNames = new Dictionary<string, List<string>>();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = "[" + (string)reader["TABLE_SCHEMA"] + "].[" + (string)reader["TABLE_NAME"] + "]";

                            if (Mode == MsSqlDropForeignKeysJobMode.SpecifiedTables && !tablesNamesHashSet.Contains(tableName))
                                continue;

                            if (!tableConstraintNames.TryGetValue(tableName, out var list))
                            {
                                list = new List<string>();
                                tableConstraintNames.Add(tableName, list);
                            }

                            list.Add("[" + (string)reader["CONSTRAINT_NAME"] + "]");
                        }
                    }

                    _tableNames = new List<string>();
                    var statements = new List<string>();
                    foreach (var kvp in tableConstraintNames.OrderBy(x => x.Key))
                    {
                        _tableNames.Add(kvp.Key);

                        statements.Add("ALTER TABLE " + kvp.Key + " DROP CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
                    }

                    var modeInfo = Mode switch
                    {
                        MsSqlDropForeignKeysJobMode.All => " (all foreign keys in database)",
                        MsSqlDropForeignKeysJobMode.SpecifiedSchema => " (in schema '" + SchemaName + "')",
                        _ => null,
                    };

                    process.Context.Log(LogSeverity.Information, process, "{ForeignKeyCount} foreign keys aquired from information schema in {Elapsed} for {TableCount} tables" + modeInfo, tableConstraintNames.Sum(x => x.Value.Count), startedOn.Elapsed, _tableNames.Count);

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
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");

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