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

    public enum MsSqlDropViewsJobMode { All, SpecifiedViews, SpecifiedSchema }

    public class MsSqlDropViewsJob : AbstractSqlStatementsJob
    {
        /// <summary>
        /// Default value is <see cref="MsSqlDropViewsJobMode.SpecifiedViews"/>
        /// </summary>
        public MsSqlDropViewsJobMode Mode { get; set; } = MsSqlDropViewsJobMode.SpecifiedViews;

        public string SchemaName { get; set; }
        public string[] ViewNames { get; set; }

        private List<string> _viewNames;

        protected override void Validate(IProcess process)
        {
            switch (Mode)
            {
                case MsSqlDropViewsJobMode.SpecifiedViews:
                    if (ViewNames == null || ViewNames.Length == 0)
                        throw new JobParameterNullException(process, this, nameof(ViewNames));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsJobMode.SpecifiedViews));
                    break;
                case MsSqlDropViewsJobMode.All:
                    if (ViewNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsJobMode.All));
                    if (!string.IsNullOrEmpty(SchemaName))
                        throw new InvalidJobParameterException(process, this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsJobMode.All));
                    break;
                case MsSqlDropViewsJobMode.SpecifiedSchema:
                    if (ViewNames != null)
                        throw new InvalidJobParameterException(process, this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsJobMode.All));
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
            switch (Mode)
            {
                case MsSqlDropViewsJobMode.SpecifiedViews:
                    _viewNames = ViewNames.ToList();
                    break;

                case MsSqlDropViewsJobMode.SpecifiedSchema:
                case MsSqlDropViewsJobMode.All:
                    var startedOn = Stopwatch.StartNew();
                    using (var command = connection.CreateCommand())
                    {
                        try
                        {
                            command.CommandTimeout = CommandTimeout;
                            command.CommandText = "select * from INFORMATION_SCHEMA.VIEWS";
                            if (Mode == MsSqlDropViewsJobMode.SpecifiedSchema)
                            {
                                command.CommandText += " where TABLE_SCHEMA = @schemaName";
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = "schemaName";
                                parameter.Value = SchemaName;
                                command.Parameters.Add(parameter);
                            }

                            process.Context.Log(LogSeverity.Debug, process, "({Job}) querying view names from {ConnectionStringKey} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                                Name, ConnectionString.Name, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");

                            _viewNames = new List<string>();
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    _viewNames.Add("[" + (string)reader["TABLE_SCHEMA"] + "].[" + (string)reader["TABLE_NAME"] + "]");
                                }
                            }

                            _viewNames.Sort();

                            var modeInfo = Mode switch
                            {
                                MsSqlDropViewsJobMode.All => " (all views in database)",
                                MsSqlDropViewsJobMode.SpecifiedSchema => " (in schema '" + SchemaName + "')",
                                _ => null,
                            };

                            process.Context.Log(LogSeverity.Information, process, "{ViewCount} views aquired from information schema in {Elapsed}" + modeInfo, _viewNames.Count, startedOn.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            var exception = new JobExecutionException(process, this, "failed to query view names from information schema", ex);
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

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var viewName = _viewNames[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "({Job}) drop view {ConnectionStringKey}/{ViewName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeViewName(viewName), command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Debug, process, "({Job}) view {ConnectionStringKey}/{ViewName} is dropped in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeViewName(viewName), startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to drop view", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop view, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeViewName(viewName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("ViewName", Helpers.UnEscapeViewName(viewName));
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

            process.Context.Log(LogSeverity.Information, process, "({Job}) {ViewCount} view(s) successfully dropped on {ConnectionStringKey} in {Elapsed}: {ViewNames}",
                 Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed,
                 _viewNames
                    .Take(lastSucceededIndex + 1)
                    .Select(Helpers.UnEscapeViewName)
                    .ToArray());
        }
    }
}