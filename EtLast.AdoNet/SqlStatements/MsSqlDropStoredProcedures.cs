namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public enum MsSqlDropStoredProceduresProcessMode { All, SpecifiedStoredProcedures, InSpecifiedSchema }

    public class MsSqlDropStoredProcedures : AbstractSqlStatements
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures"/>
        /// </summary>
        public MsSqlDropStoredProceduresProcessMode Mode { get; set; } = MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures;

        public string SchemaName { get; set; }

        /// <summary>
        /// Stored procedure names must include schema name.
        /// </summary>
        public string[] StoredProcedureNames { get; set; }

        private List<string> _storedProcedureNames;

        public MsSqlDropStoredProcedures(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            switch (Mode)
            {
                case MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures:
                    if (StoredProcedureNames == null || StoredProcedureNames.Length == 0)
                        throw new ProcessParameterNullException(this, nameof(StoredProcedureNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures));
                    break;
                case MsSqlDropStoredProceduresProcessMode.All:
                    if (StoredProcedureNames != null)
                        throw new InvalidProcessParameterException(this, nameof(StoredProcedureNames), StoredProcedureNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
                    break;
                case MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema:
                    if (StoredProcedureNames != null)
                        throw new InvalidProcessParameterException(this, nameof(StoredProcedureNames), StoredProcedureNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
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
                case MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures:
                    _storedProcedureNames = StoredProcedureNames.ToList();
                    break;

                case MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema:
                case MsSqlDropStoredProceduresProcessMode.All:
                    var startedOn = Stopwatch.StartNew();
                    using (var command = connection.CreateCommand())
                    {
                        var parameters = new Dictionary<string, object>();

                        command.CommandTimeout = CommandTimeout;
                        command.CommandText = "select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'PROCEDURE'";
                        if (Mode == MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema)
                        {
                            command.CommandText += " AND ROUTINE_SCHEMA = @schemaName";
                            parameters.Add("schemaName", SchemaName);
                        }

                        foreach (var kvp in parameters)
                        {
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = kvp.Key;
                            parameter.Value = kvp.Value;
                            command.Parameters.Add(parameter);
                        }

                        _storedProcedureNames = new List<string>();

                        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadMeta, ConnectionString.Name, "INFORMATION_SCHEMA.ROUTINES", command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                            "querying stored procedures names from {ConnectionStringName}",
                            ConnectionString.Name);

                        try
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _storedProcedureNames.Add(ConnectionString.Escape((string)reader["ROUTINE_NAME"], (string)reader["ROUTINE_SCHEMA"]));
                                }

                                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadMeta, iocUid, _storedProcedureNames.Count);
                            }

                            _storedProcedureNames.Sort();
                        }
                        catch (Exception ex)
                        {
                            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadMeta, iocUid, null, ex);

                            var exception = new ProcessExecutionException(this, "failed to query stored procedure names from information schema", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "stored procedure list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
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

            return _storedProcedureNames
                .Select(storedProcedureName => "DROP PROCEDURE IF EXISTS " + storedProcedureName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var storedProcedureName = _storedProcedureNames[statementIndex];
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(storedProcedureName), command.CommandTimeout, command.CommandText, transactionId, null,
                "drop strored procedure {ConnectionStringName}/{StoredProcedureName}",
                ConnectionString.Name, ConnectionString.Unescape(storedProcedureName));

            try
            {
                command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "failed to drop stored procedure", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop stored procedure, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(storedProcedureName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("StoredProcedureName", ConnectionString.Unescape(storedProcedureName));
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

            Context.Log(transactionId, LogSeverity.Debug, this, "{StoredProcedureCount} stored procedure(s) successfully dropped on {ConnectionStringName} in {Elapsed}", lastSucceededIndex + 1,
                ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}