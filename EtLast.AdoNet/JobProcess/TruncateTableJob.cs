namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;

    public class TruncateTableJob : AbstractSqlStatementJob
    {
        public string TableName { get; set; }

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(TableName))
                throw new JobParameterNullException(process, this, nameof(TableName));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            return "TRUNCATE TABLE " + TableName;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "truncating {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, TableName, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            var originalStatement = command.CommandText;

            try
            {
                command.CommandText = "SELECT COUNT(*) FROM " + TableName;
                var recordCount = command.ExecuteScalar();

                command.CommandText = originalStatement;
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records deleted in {ConnectionStringKey}/{TableName} in {Elapsed}", recordCount, ConnectionStringSettings.Name, TableName, startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database table truncate failed", ex);
                exception.AddOpsMessage(string.Format("database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionStringSettings.Name, TableName, ex.Message, originalStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("TableName", TableName);
                exception.Data.Add("Statement", originalStatement);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}