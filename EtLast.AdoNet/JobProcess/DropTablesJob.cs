namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;

    public class DropTablesJob : AbstractSqlStatementJob
    {
        public string[] TableNames { get; set; }

        protected override void Validate(IProcess process)
        {
            if (TableNames == null || TableNames.Length == 0)
                throw new JobParameterNullException(process, this, nameof(TableNames));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            //return string.Join("\n", TableNames.Select(x => "DROP TABLE IF EXISTS " + x + ";"));
            return "DROP TABLE IF EXISTS " + string.Join(",", TableNames) + ";";
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "dropping multiple tables {ConnectionStringKey}/{TableNames} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, TableNames, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "tables {ConnectionStringKey}/{TableNames} dropped", ConnectionStringSettings.Name, TableNames);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database multiple table drop failed", ex);
                exception.AddOpsMessage(string.Format("database multiple table drop failed, connection string key: {0}, tables: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionStringSettings.Name, string.Join(",", TableNames), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("TableNames", string.Join(",", TableNames));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}