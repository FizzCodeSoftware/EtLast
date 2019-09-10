namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    public class MsSqlDisableConstraintCheckJob : AbstractSqlStatementsJob
    {
        public string[] TableNames { get; set; }

        protected override void Validate(IProcess process)
        {
            if (TableNames == null || TableNames.Length == 0)
                throw new JobParameterNullException(process, this, nameof(TableNames));
        }

        protected override List<string> CreateSqlStatements(IProcess process, ConnectionStringSettings settings)
        {
            return TableNames.Select(tableName => "ALTER TABLE " + tableName + " NOCHECK CONSTRAINT ALL;").ToList();
        }

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "disable constraint check on {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, TableNames[statementIndex], command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "constraint check on {ConnectionStringKey}/{TableName} is disabled", ConnectionStringSettings.Name, TableNames[statementIndex]);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to disable constraint check", ex);
                exception.AddOpsMessage(string.Format("failed to disable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionStringSettings.Name, TableNames[statementIndex], ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("TableName", TableNames[statementIndex]);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}