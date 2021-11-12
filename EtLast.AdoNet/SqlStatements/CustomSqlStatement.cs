namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;

    public sealed class CustomSqlStatement : AbstractSqlStatement
    {
        public string SqlStatement { get; init; }
        public string MainTableName { get; init; }
        public Dictionary<string, object> Parameters { get; init; }

        /// <summary>
        /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
        /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
        /// Default value is true.
        /// </summary>
        public bool InlineArrayParameters { get; init; } = true;

        public CustomSqlStatement(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(SqlStatement))
                throw new ProcessParameterNullException(this, nameof(SqlStatement));
        }

        protected override string CreateSqlStatement(Dictionary<string, object> parameters)
        {
            if (Parameters != null)
            {
                foreach (var p in Parameters)
                    parameters.Add(p.Key, p.Value);
            }

            var sqlStatementProcessed = InlineArrayParametersIfNecessary(SqlStatement);
            return sqlStatementProcessed;
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = MainTableName != null
                ? Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, MainTableName, command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                    "executing custom SQL statement on {ConnectionStringName}/{TableName}",
                    ConnectionString.Name)
                : Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                    "executing custom SQL statement on {ConnectionStringName}",
                    ConnectionString.Name);

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbRead, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbRead, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "custom SQL statement failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "custom SQL statement failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                    ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }

        private string InlineArrayParametersIfNecessary(string sqlStatement)
        {
            if (InlineArrayParameters && Parameters != null)
            {
                var parameters = Parameters.ToList();
                foreach (var kvp in parameters)
                {
                    var paramReference = "@" + kvp.Key;
                    var idx = sqlStatement.IndexOf(paramReference, StringComparison.InvariantCultureIgnoreCase);
                    if (idx == -1)
                        continue;

                    if (kvp.Value is int[] intArray)
                    {
                        var newParamText = string.Join(",", intArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement[(idx + paramReference.Length)..];

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is long[] longArray)
                    {
                        var newParamText = string.Join(",", longArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement[(idx + paramReference.Length)..];

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is List<int> intList)
                    {
                        var newParamText = string.Join(",", intList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement[(idx + paramReference.Length)..];

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is List<long> longList)
                    {
                        var newParamText = string.Join(",", longList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement[(idx + paramReference.Length)..];

                        Parameters.Remove(kvp.Key);
                    }
                }
            }

            return sqlStatement;
        }
    }
}