namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class DeleteTableJob : AbstractSqlStatementJob
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        protected override void Validate()
        {
            if (string.IsNullOrEmpty(TableName))
                throw new JobParameterNullException(Process, this, nameof(TableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString)
        {
            return string.IsNullOrEmpty(CustomWhereClause)
                ? "DELETE FROM " + TableName
                : "DELETE FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override void RunCommand(IDbCommand command, Stopwatch startedOn)
        {
            Process.Context.Log(LogSeverity.Debug, Process, this, null, "deleting records from {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, Helpers.UnEscapeTableName(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Process.Context.Log(LogSeverity.Information, Process, this, null, "{RecordCount} records deleted in {ConnectionStringKey}/{TableName} in {Elapsed}, transaction: {Transaction}",
                    recordCount, ConnectionString.Name, Helpers.UnEscapeTableName(TableName), startedOn.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "database table content deletion failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table content deletion failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(TableName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", Helpers.UnEscapeTableName(TableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}