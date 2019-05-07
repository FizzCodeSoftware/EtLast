namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;

    public class CustomSqlStatementJob : AbstractSqlStatementJob
    {
        public string SqlStatement { get; set; }

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(SqlStatement)) throw new JobParameterNullException(process, this, nameof(SqlStatement));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            return SqlStatement;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "executing custom sql statement {SqlStatement} on {ConnectionStringKey}, timeout: {Timeout} sec, transaction: {Transaction}",
                command.CommandText, ConnectionStringKey, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                var recordCount = command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records affected in {Elapsed}", recordCount, startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "custom sql statement failed", ex);
                exception.AddOpsMessage(string.Format("custom sql statement failed, connection string key: {0}, message {1}, command: {2}, timeout: {3}",
                    ConnectionStringKey, ex.Message, SqlStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                exception.Data.Add("Statement", SqlStatement);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}