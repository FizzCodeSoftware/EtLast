namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Transactions;

    public class GetTableMaxValueProcess<T> : AbstractSqlStatementWithResultProcess<TableMaxValueResult<T>>
    {
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string CustomWhereClause { get; set; }

        public GetTableMaxValueProcess(ITopic topic, string name)
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

        protected override TableMaxValueResult<T> RunCommandAndGetResult(IDbCommand command)
        {
            Context.LogNoDiag(LogSeverity.Debug, this, "getting max value from {ConnectionStringName}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var result = new TableMaxValueResult<T>();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var mv = reader["maxValue"];
                        if (!(mv is DBNull))
                        {
                            result.MaxValue = (T)mv;
                        }

                        result.RecordCount = (int)reader["cnt"];
                    }
                }

                Context.Log(LogSeverity.Debug, this, "maximum value {MaxValue} and {RecordCount} records found in {ConnectionStringName}/{TableName} in column {ColumnName} in {Elapsed}, transaction: {Transaction}",
                    result.MaxValue, result.RecordCount, ConnectionString.Name, ConnectionString.Unescape(TableName), ConnectionString.Unescape(ColumnName), InvocationInfo.LastInvocationStarted.Elapsed, Transaction.Current.ToIdentifierString());

                return result;
            }
            catch (Exception ex)
            {
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