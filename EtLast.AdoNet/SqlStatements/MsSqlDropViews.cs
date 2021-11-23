namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    public enum MsSqlDropViewsProcessMode { All, SpecifiedViews, SpecifiedSchema }

    public sealed class MsSqlDropViews : AbstractSqlStatements
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropViewsProcessMode.SpecifiedViews"/>
        /// </summary>
        public MsSqlDropViewsProcessMode Mode { get; init; } = MsSqlDropViewsProcessMode.SpecifiedViews;

        public string SchemaName { get; init; }
        public string[] ViewNames { get; init; }

        private List<string> _viewNames;

        public MsSqlDropViews(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            switch (Mode)
            {
                case MsSqlDropViewsProcessMode.SpecifiedViews:
                    if (ViewNames == null || ViewNames.Length == 0)
                        throw new ProcessParameterNullException(this, nameof(ViewNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.SpecifiedViews));
                    break;
                case MsSqlDropViewsProcessMode.All:
                    if (ViewNames != null)
                        throw new InvalidProcessParameterException(this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                    break;
                case MsSqlDropViewsProcessMode.SpecifiedSchema:
                    if (ViewNames != null)
                        throw new InvalidProcessParameterException(this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                    if (string.IsNullOrEmpty(SchemaName))
                        throw new ProcessParameterNullException(this, nameof(SchemaName));
                    break;
            }

            if (ConnectionString.SqlEngine != SqlEngine.MsSql)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
        {
            switch (Mode)
            {
                case MsSqlDropViewsProcessMode.SpecifiedViews:
                    _viewNames = ViewNames.ToList();
                    break;

                case MsSqlDropViewsProcessMode.SpecifiedSchema:
                case MsSqlDropViewsProcessMode.All:
                    var startedOn = Stopwatch.StartNew();
                    using (var command = connection.CreateCommand())
                    {
                        var parameters = new Dictionary<string, object>();

                        command.CommandTimeout = CommandTimeout;
                        command.CommandText = "select * from INFORMATION_SCHEMA.VIEWS";
                        if (Mode == MsSqlDropViewsProcessMode.SpecifiedSchema)
                        {
                            command.CommandText += " where TABLE_SCHEMA = @schemaName";
                            parameters.Add("schemaName", SchemaName);
                        }

                        foreach (var kvp in parameters)
                        {
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = kvp.Key;
                            parameter.Value = kvp.Value;
                            command.Parameters.Add(parameter);
                        }

                        _viewNames = new List<string>();

                        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadMeta, ConnectionString.Name, "INFORMATION_SCHEMA.VIEWS", command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                            "querying view names from {ConnectionStringName}",
                            ConnectionString.Name);

                        try
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _viewNames.Add(ConnectionString.Escape((string)reader["TABLE_NAME"], (string)reader["TABLE_SCHEMA"]));
                                }

                                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadMeta, iocUid, _viewNames.Count);
                            }

                            _viewNames.Sort();
                        }
                        catch (Exception ex)
                        {
                            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadMeta, iocUid, null, ex);

                            var exception = new SqlSchemaReadException(this, "view names", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "view list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
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

            return _viewNames
                .ConvertAll(viewName => "DROP VIEW IF EXISTS " + viewName + ";")
;
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var viewName = _viewNames[statementIndex];
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(viewName), command.CommandTimeout, command.CommandText, transactionId, null,
                "drop view {ConnectionStringName}/{ViewName}",
                ConnectionString.Name, ConnectionString.Unescape(viewName));

            try
            {
                command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

                var exception = new SqlSchemaChangeException(this, "drop view", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop view, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(viewName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("ViewName", ConnectionString.Unescape(viewName));
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

            Context.Log(transactionId, LogSeverity.Debug, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName} in {Elapsed}", lastSucceededIndex + 1,
                ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}