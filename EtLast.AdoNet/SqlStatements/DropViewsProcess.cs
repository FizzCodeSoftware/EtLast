﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public class DropViewsProcess : AbstractSqlStatementsProcess
    {
        public string[] TableNames { get; set; }

        public DropViewsProcess(ITopic topic, string name)
            : base(topic, name)
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
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient or MySql.Data.MySqlClient");
            }
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
        {
            return TableNames.Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";").ToList();
        }

        protected override void LogAction(int statementIndex, string transactionId)
        {
            var viewName = TableNames[statementIndex];

            Context.Log(transactionId, LogSeverity.Debug, this, "drop view {ConnectionStringName}/{ViewName}",
                ConnectionString.Name, ConnectionString.Unescape(viewName));
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var viewName = TableNames[statementIndex];
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, null);
            try
            {
                command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, null);

                var time = startedOn.Elapsed;

                Context.Log(transactionId, LogSeverity.Debug, this, "view {ConnectionStringName}/{ViewName} is dropped in {Elapsed}",
                    ConnectionString.Name, ConnectionString.Unescape(viewName), time);

                CounterCollection.IncrementCounter("db drop view count", 1);
                CounterCollection.IncrementTimeSpan("db drop view time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementCounter("db drop view count - " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementTimeSpan("db drop view time - " + ConnectionString.Name, time);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

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

            Context.Log(transactionId, LogSeverity.Debug, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName} in {Elapsed}",
                lastSucceededIndex + 1, ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}