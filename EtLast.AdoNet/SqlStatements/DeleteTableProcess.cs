namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class DeleteTableProcess : AbstractSqlStatementProcess
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        public DeleteTableProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            base.Validate();

            if (string.IsNullOrEmpty(TableName))
                throw new ProcessParameterNullException(this, nameof(TableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString)
        {
            return string.IsNullOrEmpty(CustomWhereClause)
                ? "DELETE FROM " + TableName
                : "DELETE FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override void RunCommand(IDbCommand command, Stopwatch startedOn)
        {
            Context.Log(LogSeverity.Debug, this, "deleting records from {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.Log(LogSeverity.Information, this, "{RecordCount} records deleted in {ConnectionStringKey}/{TableName} in {Elapsed}, transaction: {Transaction}", recordCount,
                    ConnectionString.Name, ConnectionString.Unescape(TableName), startedOn.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "database table content deletion failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table content deletion failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}