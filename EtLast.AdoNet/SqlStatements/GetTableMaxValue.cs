namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;

    public sealed class GetTableMaxValue<T> : AbstractSqlStatementWithResult<TableMaxValueResult<T>>
    {
        public string TableName { get; init; }
        public string ColumnName { get; init; }
        public string CustomWhereClause { get; init; }

        public GetTableMaxValue(ITopic topic, string name)
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
                ? "SELECT MAX(" + ColumnName + ") AS maxValue, COUNT(*) AS cnt FROM " + TableName
                : "SELECT MAX(" + ColumnName + ") AS maxValue, COUNT(*) AS cnt FROM " + TableName + " WHERE " + CustomWhereClause;
        }

        protected override TableMaxValueResult<T> RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadAggregate, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                "getting max value from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));

            try
            {
                var result = new TableMaxValueResult<T>();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var mv = reader["maxValue"];
                        if (mv is not DBNull)
                        {
                            result.MaxValue = (T)mv;
                        }

                        result.RecordCount = (int)reader["cnt"];
                    }
                }

                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadAggregate, iocUid, result.RecordCount);
                return result;
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadAggregate, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "database table max value query failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table max value query failed, connection string key: {0}, table: {1}, column: {2}, message: {3}, command: {4}, timeout: {5}",
                    ConnectionString.Name, ConnectionString.Unescape(TableName), ConnectionString.Unescape(ColumnName), ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
                exception.Data.Add("ColumnName", ConnectionString.Unescape(ColumnName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }

    public class TableMaxValueResult<T>
    {
        public T MaxValue { get; set; }
        public int RecordCount { get; set; }
    }
}