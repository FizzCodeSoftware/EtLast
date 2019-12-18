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

    public class DropViewsProcess : AbstractSqlStatementsProcess
    {
        public string[] TableNames { get; set; }

        public DropViewsProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableNames == null || TableNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(TableNames));

            if ((ConnectionString.KnownProvider != KnownProvider.SqlServer) &&
                (ConnectionString.KnownProvider != KnownProvider.MySql))
            {
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient or MySql.Data.MySqlClient");
            }
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return TableNames.Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";").ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var viewName = TableNames[statementIndex];

            Context.Log(LogSeverity.Debug, this, "drop view {ConnectionStringName}/{ViewName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(viewName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                var time = startedOn.Elapsed;

                Context.Log(LogSeverity.Debug, this, "view {ConnectionStringName}/{ViewName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(viewName), time, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db drop view count", 1);
                CounterCollection.IncrementTimeSpan("db drop view time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementDebugCounter("db drop view count - " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementDebugTimeSpan("db drop view time - " + ConnectionString.Name, time);
            }
            catch (Exception ex)
            {
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

        protected override void LogSucceeded(int lastSucceededIndex)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(LogSeverity.Information, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}