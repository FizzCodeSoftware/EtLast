namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public enum MsSqlDropViewsProcessMode { All, SpecifiedViews, SpecifiedSchema }

    public class MsSqlDropViews : AbstractSqlStatements
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropViewsProcessMode.SpecifiedViews"/>
        /// </summary>
        public MsSqlDropViewsProcessMode Mode { get; set; } = MsSqlDropViewsProcessMode.SpecifiedViews;

        public string SchemaName { get; set; }
        public string[] ViewNames { get; set; }

        private List<string> _viewNames;

        public MsSqlDropViews(ITopic topic, string name)
            : base(topic, name)
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

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
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

                        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, "INFORMATION_SCHEMA.VIEWS", command.CommandTimeout, command.CommandText, transactionId, () => parameters,
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

                                Context.RegisterIoCommandSuccess(this, iocUid, _viewNames.Count);
                            }

                            _viewNames.Sort();
                        }
                        catch (Exception ex)
                        {
                            Context.RegisterIoCommandFailed(this, iocUid, null, ex);

                            var exception = new ProcessExecutionException(this, "failed to query view names from information schema", ex);
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
                .Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var viewName = _viewNames[statementIndex];
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbDefinition, ConnectionString.Name, ConnectionString.Unescape(viewName), command.CommandTimeout, command.CommandText, transactionId, null,
                "drop view {ConnectionStringName}/{ViewName}",
                ConnectionString.Name, ConnectionString.Unescape(viewName));

            try
            {
                command.ExecuteNonQuery();
                var time = startedOn.Elapsed;

                Context.RegisterIoCommandSuccess(this, iocUid, null);

                CounterCollection.IncrementCounter("db drop view count", 1);
                CounterCollection.IncrementTimeSpan("db drop view time", time);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "failed to drop view", ex);
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