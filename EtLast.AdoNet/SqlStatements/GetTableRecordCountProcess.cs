namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public class GetTableRecordCountProcess : AbstractSqlStatementWithResultProcess<int>
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        public GetTableRecordCountProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(TableName))
                throw new ProcessParameterNullException(this, nameof(TableName));
        }

        protected override string CreateSqlStatement(Dictionary<string, object> parameters)
        {
            return string.IsNullOrEmpty(CustomWhereClause)
                ? "SELECT COUNT(*) FROM " + TableName
                : "SELECT COUNT(*) FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "getting record count from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));
        }

        protected override int RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.read, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters);

            try
            {
                var result = command.ExecuteScalar();
                if (!(result is int recordCount))
                    recordCount = 0;

                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);

                Context.Log(transactionId, LogSeverity.Debug, this, "{RecordCount} records found in {ConnectionStringName}/{TableName} in {Elapsed}",
                    recordCount, ConnectionString.Name, ConnectionString.Unescape(TableName), InvocationInfo.LastInvocationStarted.Elapsed);

                return recordCount;
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "database table record count query failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table record count query failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}