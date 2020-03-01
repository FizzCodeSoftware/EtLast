namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public class TruncateTableProcess : AbstractSqlStatementProcess
    {
        public string TableName { get; set; }

        public TruncateTableProcess(ITopic topic, string name)
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
            return "TRUNCATE TABLE " + TableName;
        }

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "truncating {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var originalStatement = command.CommandText;

            var recordCount = 0;
            command.CommandText = "SELECT COUNT(*) FROM " + TableName;
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.read, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, null);
            try
            {
                recordCount = (int)command.ExecuteScalar();
                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "database table truncate failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }

            command.CommandText = originalStatement;
            dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters);
            try
            {
                command.ExecuteNonQuery();
                Context.Log(transactionId, LogSeverity.Information, this, "{RecordCount} records deleted in {ConnectionStringName}/{TableName} in {Elapsed}",
                    recordCount, ConnectionString.Name, TableName, InvocationInfo.LastInvocationStarted.Elapsed);
                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "database table truncate failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("Statement", originalStatement);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}