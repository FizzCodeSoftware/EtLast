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

    public class MsSqlEnableConstraintCheckJob : AbstractSqlStatementsJob
    {
        public string[] TableNames { get; set; }

        protected override void Validate()
        {
            if (TableNames == null || TableNames.Length == 0)
                throw new JobParameterNullException(Process, this, nameof(TableNames));
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return TableNames.Select(tableName => "ALTER TABLE " + tableName + " WITH CHECK CHECK CONSTRAINT ALL;").ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = TableNames[statementIndex];

            Process.Context.Log(LogSeverity.Debug, Process, "({Job}) enable constraint check on {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();
                Process.Context.Log(LogSeverity.Debug, Process, "({Job}) constraint check on {ConnectionStringKey}/{TableName} is enabled in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "failed to enable constraint check", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to enable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, tableName, ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", Helpers.UnEscapeTableName(tableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            Process.Context.Log(LogSeverity.Information, Process, "({Job}) constraint check successfully enabled on {TableCount} tables on {ConnectionStringKey} in {Elapsed}",
                Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed);
        }
    }
}