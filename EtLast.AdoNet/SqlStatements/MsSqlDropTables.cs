namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public enum MsSqlDropTablesProcessMode { All, SpecifiedTables, SpecifiedSchema }

    public class MsSqlDropTables : AbstractSqlStatements
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropTablesProcessMode.SpecifiedTables"/>
        /// </summary>
        public MsSqlDropTablesProcessMode Mode { get; set; } = MsSqlDropTablesProcessMode.SpecifiedTables;

        public string SchemaName { get; set; }
        public string[] TableNames { get; set; }

        private List<string> _tableNames;

        public MsSqlDropTables(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
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

            if (ConnectionString.SqlEngine != SqlEngine.MsSql)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
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
                        var parameters = new Dictionary<string, object>();

                        command.CommandTimeout = CommandTimeout;
                        command.CommandText = "select * from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE'";
                        if (Mode == MsSqlDropTablesProcessMode.SpecifiedSchema)
                        {
                            command.CommandText += " and TABLE_SCHEMA = @schemaName";
                            parameters.Add("schemaName", SchemaName);
                        }

                        command.FillCommandParameters(parameters);

                        _tableNames = new List<string>();

                        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, "INFORMATION_SCHEMA.TABLES", command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                            "querying table names from {ConnectionStringName}",
                            ConnectionString.Name);

                        try
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _tableNames.Add(ConnectionString.Escape((string)reader["TABLE_NAME"], (string)reader["TABLE_SCHEMA"]));
                                }

                                Context.RegisterIoCommandSuccess(this, iocUid, _tableNames.Count);
                            }

                            _tableNames.Sort();
                        }
                        catch (Exception ex)
                        {
                            Context.RegisterIoCommandFailed(this, iocUid, null, ex);

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
                .Select(tableName => "DROP TABLE " + tableName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var tableName = _tableNames[statementIndex];
            var originalStatement = command.CommandText;

            var recordCount = 0;
            command.CommandText = "SELECT COUNT(*) FROM " + tableName;
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
                "querying record count from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(tableName));
            try
            {
                recordCount = (int)command.ExecuteScalar();
                Context.RegisterIoCommandSuccess(this, iocUid, recordCount);
            }
            catch (Exception)
            {
                Context.RegisterIoCommandSuccess(this, iocUid, null);
            }

            command.CommandText = originalStatement;
            iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbDefinition, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
                "drop table {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(tableName));

            try
            {
                command.ExecuteNonQuery();
                var time = startedOn.Elapsed;

                Context.RegisterIoCommandSuccess(this, iocUid, recordCount);

                CounterCollection.IncrementCounter("db drop table count", 1);
                CounterCollection.IncrementTimeSpan("db drop table time", time);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, null, ex);

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

        protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(transactionId, LogSeverity.Debug, this, "{TableCount} table(s) successfully dropped on {ConnectionStringName} in {Elapsed}", lastSucceededIndex + 1,
                ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}