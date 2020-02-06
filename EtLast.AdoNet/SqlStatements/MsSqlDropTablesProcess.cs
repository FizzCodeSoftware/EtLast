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

    public enum MsSqlDropTablesProcessMode { All, SpecifiedTables, SpecifiedSchema }

    public class MsSqlDropTablesProcess : AbstractSqlStatementsProcess
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropTablesProcessMode.SpecifiedTables"/>
        /// </summary>
        public MsSqlDropTablesProcessMode Mode { get; set; } = MsSqlDropTablesProcessMode.SpecifiedTables;

        public string SchemaName { get; set; }
        public string[] TableNames { get; set; }

        private List<string> _tableNames;

        public MsSqlDropTablesProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public override void ValidateImpl()
        {
            base.ValidateImpl();

            switch (Mode)
            {
                case MsSqlDropTablesProcessMode.SpecifiedTables:
                    if (TableNames == null || TableNames.Length == 0)
                        throw new ProcessParameterNullException(this, nameof(TableNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesProcessMode.SpecifiedTables));
                    break;
                case MsSqlDropTablesProcessMode.All:
                    if (TableNames != null)
                        throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesProcessMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesProcessMode.All));
                    break;
                case MsSqlDropTablesProcessMode.SpecifiedSchema:
                    if (TableNames != null)
                        throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropTablesProcessMode.All));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new ProcessParameterNullException(this, nameof(SchemaName));
                    break;
            }

            if (ConnectionString.KnownProvider != KnownProvider.SqlServer)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            switch (Mode)
            {
                case MsSqlDropTablesProcessMode.SpecifiedTables:
                    _tableNames = TableNames.ToList();
                    break;

                case MsSqlDropTablesProcessMode.SpecifiedSchema:
                case MsSqlDropTablesProcessMode.All:
                    var startedOn = Stopwatch.StartNew();
                    using (var command = connection.CreateCommand())
                    {
                        try
                        {
                            var parameters = new Dictionary<string, object>();

                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = "select * from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE'";
                            if (Mode == MsSqlDropTablesProcessMode.SpecifiedSchema)
                            {
                                command.CommandText += " and TABLE_SCHEMA = @schemaName";
                                parameters.Add("schemaName", SchemaName);
                            }

                            foreach (var kvp in parameters)
                            {
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = kvp.Key;
                                parameter.Value = kvp.Value;
                                command.Parameters.Add(parameter);
                            }

                            Context.LogDataStoreCommand(ConnectionString.Name, this, null, command.CommandText, parameters);

                            Context.Log(LogSeverity.Debug, this, "querying table names from {ConnectionStringName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                                command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

                            _tableNames = new List<string>();
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _tableNames.Add(ConnectionString.Escape((string)reader["TABLE_NAME"], (string)reader["TABLE_SCHEMA"]));
                                }
                            }

                            _tableNames.Sort();

                            var modeInfo = Mode switch
                            {
                                MsSqlDropTablesProcessMode.All => " (all tables in database)",
                                MsSqlDropTablesProcessMode.SpecifiedSchema => " (in schema '" + SchemaName + "')",
                                _ => null,
                            };

                            Context.Log(LogSeverity.Information, this, "{TableCount} tables aquired from information schema of {ConnectionStringName} in {Elapsed}" + modeInfo,
                                _tableNames.Count, ConnectionString.Name, startedOn.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            var exception = new ProcessExecutionException(this, "failed to query table names from information schema", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "table list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
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

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = _tableNames[statementIndex];

            Context.Log(LogSeverity.Debug, this, "drop table {ConnectionStringName}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                var time = startedOn.Elapsed;

                Context.Log(LogSeverity.Debug, this, "table {ConnectionStringName}/{TableName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(tableName), time, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db drop table count", 1);
                CounterCollection.IncrementTimeSpan("db drop table time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementCounter("db drop table count - " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementTimeSpan("db drop table time - " + ConnectionString.Name, time);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "failed to drop table", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(tableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(LogSeverity.Information, this, "{TableCount} table(s) successfully dropped on {ConnectionStringName} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}