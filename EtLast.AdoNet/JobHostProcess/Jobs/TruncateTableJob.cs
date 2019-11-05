namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class TruncateTableJob : AbstractSqlStatementJob
    {
        public string TableName { get; set; }

        protected override void Validate()
        {
            if (string.IsNullOrEmpty(TableName))
                throw new JobParameterNullException(Process, this, nameof(TableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString)
        {
            return "TRUNCATE TABLE " + TableName;
        }

        protected override void RunCommand(IDbCommand command, Stopwatch startedOn)
        {
            Process.Context.Log(LogSeverity.Debug, Process, this, null, "truncating {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            var originalStatement = command.CommandText;

            try
            {
                command.CommandText = "SELECT COUNT(*) FROM " + TableName;
                var recordCount = command.ExecuteScalar();

                command.CommandText = originalStatement;
                command.ExecuteNonQuery();
                Process.Context.Log(LogSeverity.Information, Process, this, null, "{RecordCount} records deleted in {ConnectionStringKey}/{TableName} in {Elapsed}, transaction: {Transaction}",
                    recordCount, ConnectionString.Name, TableName, startedOn.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "database table truncate failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", originalStatement);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}