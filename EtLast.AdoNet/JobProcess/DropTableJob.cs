namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;

    public class DropTableJob : AbstractSqlStatementJob
    {
        public string TableName { get; set; }

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(TableName))
                throw new JobParameterNullException(process, this, nameof(TableName));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            return "DROP TABLE IF EXISTS " + TableName;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "dropping table {ConnectionStringKey}/{TableName} with query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringKey, TableName, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "table {ConnectionStringKey}/{TableName} dropped", ConnectionStringKey, TableName);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database table drop failed", ex);
                exception.AddOpsMessage(string.Format("database table drop failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionStringKey, TableName, ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                exception.Data.Add("TableName", TableName);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}