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

    public enum MsSqlDropViewsProcessMode { All, SpecifiedViews, SpecifiedSchema }

    public class MsSqlDropViewsProcess : AbstractSqlStatementsProcess
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropViewsProcessMode.SpecifiedViews"/>
        /// </summary>
        public MsSqlDropViewsProcessMode Mode { get; set; } = MsSqlDropViewsProcessMode.SpecifiedViews;

        public string SchemaName { get; set; }
        public string[] ViewNames { get; set; }

        private List<string> _viewNames;

        public MsSqlDropViewsProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
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

            var knownProvider = Context.GetConnectionString(ConnectionStringKey)?.KnownProvider;
            if (knownProvider != KnownProvider.SqlServer)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
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
                        try
                        {
                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = "select * from INFORMATION_SCHEMA.VIEWS";
                            if (Mode == MsSqlDropViewsProcessMode.SpecifiedSchema)
                            {
                                command.CommandText += " where TABLE_SCHEMA = @schemaName";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "schemaName";
                                parameter.Value = SchemaName;
                                command.Parameters.Add(parameter);
                            }

                            Context.Log(LogSeverity.Debug, this, "querying view names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                                command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

                            _viewNames = new List<string>();
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _viewNames.Add(ConnectionString.Escape((string)reader["TABLE_NAME"], (string)reader["TABLE_SCHEMA"]));
                                }
                            }

                            _viewNames.Sort();

                            var modeInfo = Mode switch
                            {
                                MsSqlDropViewsProcessMode.All => " (all views in database)",
                                MsSqlDropViewsProcessMode.SpecifiedSchema => " (in schema '" + SchemaName + "')",
                                _ => null,
                            };

                            Context.Log(LogSeverity.Information, this, "{ViewCount} views aquired from information schema of {ConnectionStringKey} in {Elapsed}" + modeInfo,
                                _viewNames.Count, ConnectionString.Name, startedOn.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            var exception = new ProcessExecutionException(this, "failed to query view names from information schema", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "view list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
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

            return _viewNames
                .Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex)
        {
            var viewName = _viewNames[statementIndex];

            Context.Log(LogSeverity.Debug, this, "drop view {ConnectionStringKey}/{ViewName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(viewName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                Context.Log(LogSeverity.Debug, this, "view {ConnectionStringKey}/{ViewName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(viewName), LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db views dropped", 1);
                CounterCollection.IncrementCounter("db views dropped time", LastInvocation.ElapsedMilliseconds);

                // not relevant on process level
                Context.CounterCollection.IncrementCounter("db views dropped / " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementCounter("db views dropped time / " + ConnectionString.Name, LastInvocation.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "failed to drop view", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop view, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(viewName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("ViewName", ConnectionString.Unescape(viewName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocation.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(LogSeverity.Information, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringKey} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}