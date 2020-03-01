namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public class DeleteTableProcess : AbstractSqlStatementProcess
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        public DeleteTableProcess(ITopic topic, string name)
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
                ? "DELETE FROM " + TableName
                : "DELETE FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "deleting records from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters);
            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);

                Context.Log(transactionId, LogSeverity.Debug, this, "{RecordCount} records deleted in {ConnectionStringName}/{TableName} in {Elapsed}",
                    recordCount, ConnectionString.Name, ConnectionString.Unescape(TableName), InvocationInfo.LastInvocationStarted.Elapsed);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "database table content deletion failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table content deletion failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
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