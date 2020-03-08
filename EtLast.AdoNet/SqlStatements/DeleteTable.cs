namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public class DeleteTable : AbstractSqlStatement
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }

        public DeleteTable(ITopic topic, string name)
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

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbDelete, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                "deleting records from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, null, ex);

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