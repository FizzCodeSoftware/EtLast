namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class TruncateTableProcess : AbstractSqlStatementProcess
    {
        public string TableName { get; set; }

        public TruncateTableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(TableName))
                throw new ProcessParameterNullException(this, nameof(TableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters)
        {
            return "TRUNCATE TABLE " + TableName;
        }

        protected override void RunCommand(IDbCommand command)
        {
            Context.Log(LogSeverity.Debug, this, "truncating {ConnectionStringName}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            var originalStatement = command.CommandText;

            try
            {
                command.CommandText = "SELECT COUNT(*) FROM " + TableName;
                var recordCount = command.ExecuteScalar();

                command.CommandText = originalStatement;
                command.ExecuteNonQuery();
                Context.Log(LogSeverity.Information, this, "{RecordCount} records deleted in {ConnectionStringName}/{TableName} in {Elapsed}, transaction: {Transaction}", recordCount,
                    ConnectionString.Name, TableName, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "database table truncate failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", originalStatement);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocation.Elapsed);
                throw exception;
            }
        }
    }
}