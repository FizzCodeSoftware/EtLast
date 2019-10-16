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

    public enum MsSqlDropTablesJobMode { All, SpecifiedTables, SpecifiedSchema }

    public class MsSqlDropTablesJob : AbstractSqlStatementsJob
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropTablesJobMode.SpecifiedTables"/>
        /// </summary>
        public MsSqlDropTablesJobMode Mode { get; set; } = MsSqlDropTablesJobMode.SpecifiedTables;

        public string SchemaName { get; set; }
        public string[] TableNames { get; set; }

        private List<string> _tableNames;

        protected override void Validate(IProcess process)
        {
            switch (Mode)
            {
                case MsSqlDropTablesJobMode.SpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new JobParameterNullException(process, this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesJobMode.SpecifiedTables));
                    break;
                case MsSqlDropTablesJobMode.All:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesJobMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesJobMode.All));
                    break;
                case MsSqlDropTablesJobMode.SpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesJobMode.All));
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
            switch (Mode)
            {
                case MsSqlDropTablesJobMode.SpecifiedTables:
                    _tableNames = TableNames.ToList();
                    break;

                case MsSqlDropTablesJobMode.SpecifiedSchema:
                case MsSqlDropTablesJobMode.All:
                    var startedOn = Stopwatch.StartNew();
                    using (var command = connection.CreateCommand())
                    {
                        try
                        {
                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = "select * from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE'";
                            if (Mode == MsSqlDropTablesJobMode.SpecifiedSchema)
                            {
                                command.CommandText += " and TABLE_SCHEMA = @schemaName";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "schemaName";
                                parameter.Value = SchemaName;
                                command.Parameters.Add(parameter);
                            }

                            process.Context.Log(LogSeverity.Debug, process, "({Job}) querying table names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                                Name, ConnectionString.Name, command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

                            _tableNames = new List<string>();
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _tableNames.Add("[" + (string)reader["TABLE_SCHEMA"] + "].[" + (string)reader["TABLE_NAME"] + "]");
                                }
                            }

                            _tableNames.Sort();

                            var modeInfo = Mode switch
                            {
                                MsSqlDropTablesJobMode.All => " (all tables in database)",
                                MsSqlDropTablesJobMode.SpecifiedSchema => " (in schema '" + SchemaName + "')",
                                _ => null,
                            };

                            process.Context.Log(LogSeverity.Information, process, "{TableCount} tables aquired from information schema in {Elapsed}" + modeInfo,
                                _tableNames.Count, startedOn.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            var exception = new JobExecutionException(process, this, "failed to query table names from information schema", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "table list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                            exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                            exception.Data.Add("Statement", command.CommandText);
                            exception.Data.Add("Timeout", command.CommandTimeout);
                            exception.Data.Add("Elapsed", startedOn.Elapsed);
                            throw exception;
                        }
                    }
                    break;
            }

            return _tableNames
                .Select(tableName => "DROP TABLE IF EXISTS " + tableName + ";")
                .ToList();
        }

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = _tableNames[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "({Job}) drop table {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                process.Context.Log(LogSeverity.Debug, process, "({Job}) table {ConnectionStringKey}/{TableName} is dropped in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), startedOn.Elapsed);

                process.Context.Stat.IncrementCounter("database tables dropped / " + ConnectionString.Name, 1);
                process.Context.Stat.IncrementCounter("database tables dropped time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to drop table", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", Helpers.UnEscapeTableName(tableName));
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

            process.Context.Log(LogSeverity.Information, process, "({Job}) {TableCount} table(s) successfully dropped on {ConnectionStringKey} in {Elapsed}: {TableNames}",
                 Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed,
                 _tableNames
                    .Take(lastSucceededIndex + 1)
                    .Select(Helpers.UnEscapeTableName)
                    .ToArray());
        }
    }
}