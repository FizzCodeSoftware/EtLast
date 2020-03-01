namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public class MsSqlEnableConstraintCheckProcess : AbstractSqlStatementsProcess
    {
        public string[] TableNames { get; set; }

        public MsSqlEnableConstraintCheckProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableNames == null || TableNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(TableNames));
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
        {
            return TableNames.Select(tableName => "ALTER TABLE " + tableName + " WITH CHECK CHECK CONSTRAINT ALL;").ToList();
        }

        protected override void LogAction(int statementIndex, string transactionId)
        {
            var tableName = TableNames[statementIndex];

            Context.Log(transactionId, LogSeverity.Debug, this, "enable constraint check on {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(tableName));
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var tableName = TableNames[statementIndex];
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, null);
            try
            {
                command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, null);

                var time = startedOn.Elapsed;

                Context.Log(transactionId, LogSeverity.Debug, this, "constraint check on {ConnectionStringName}/{TableName} is enabled in {Elapsed}",
                    ConnectionString.Name, ConnectionString.Unescape(tableName), time);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "failed to enable constraint check", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to enable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, tableName, ex.Message, command.CommandText, command.CommandTimeout));

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

            Context.Log(transactionId, LogSeverity.Debug, this, "constraint check successfully enabled on {TableCount} tables on {ConnectionStringName} in {Elapsed}",
                lastSucceededIndex + 1, ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}