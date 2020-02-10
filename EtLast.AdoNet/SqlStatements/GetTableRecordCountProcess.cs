namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class GetTableRecordCountProcess : AbstractSqlStatementWithResultProcess<int>
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        public GetTableRecordCountProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(TableName))
                throw new ProcessParameterNullException(this, nameof(TableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters)
        {
            return string.IsNullOrEmpty(CustomWhereClause)
                ? "SELECT COUNT(*) FROM " + TableName
                : "SELECT COUNT(*) FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override int RunCommandAndGetResult(IDbCommand command)
        {
            Context.Log(LogSeverity.Debug, this, "getting record count from {ConnectionStringName}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var result = command.ExecuteScalar();

                if (!(result is int recordCount))
                    recordCount = 0;

                Context.Log(LogSeverity.Debug, this, "{RecordCount} records found in {ConnectionStringName}/{TableName} in {Elapsed}, transaction: {Transaction}", recordCount,
                    ConnectionString.Name, ConnectionString.Unescape(TableName), LastInvocationStarted.Elapsed, Transaction.Current.ToIdentifierString());

                return recordCount;
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "database table record count query failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table record count query failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}