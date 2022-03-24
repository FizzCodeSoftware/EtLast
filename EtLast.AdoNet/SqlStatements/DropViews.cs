namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    public sealed class DropViews : AbstractSqlStatements
    {
        public string[] TableNames { get; init; }

        public DropViews(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableNames == null || TableNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(TableNames));

            if ((ConnectionString.SqlEngine != SqlEngine.MsSql)
                && (ConnectionString.SqlEngine != SqlEngine.MySql))
            {
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient or MySql.Data.MySqlClient");
            }
        }

        protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
        {
            return TableNames.Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";").ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var viewName = TableNames[statementIndex];
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

            Context.Log(transactionId, LogSeverity.Debug, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName} in {Elapsed}",
                lastSucceededIndex + 1, ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}