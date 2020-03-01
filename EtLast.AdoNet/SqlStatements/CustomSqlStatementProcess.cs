namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;

    public class CustomSqlStatementProcess : AbstractSqlStatementProcess
    {
        public string SqlStatement { get; set; }
        public Dictionary<string, object> Parameters { get; set; }

        /// <summary>
        /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
        /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
        /// Default value is true.
        /// </summary>
        public bool InlineArrayParameters { get; set; } = true;

        public CustomSqlStatementProcess(ITopic topic, string name)
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
            foreach (var p in Parameters)
                parameters.Add(p.Key, p.Value);

            var sqlStatementProcessed = InlineArrayParametersIfNecessary(SqlStatement);
            return sqlStatementProcessed;
        }

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "executing custom SQL statement on {ConnectionStringName}",
                ConnectionString.Name);
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters);
            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);

                Context.Log(transactionId, LogSeverity.Debug, this, "custom SQL statement affected {RecordCount} records in {Elapsed}", recordCount,
                    InvocationInfo.LastInvocationStarted.Elapsed);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

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
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement.Substring(idx + paramReference.Length);

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is long[] longArray)
                    {
                        var newParamText = string.Join(",", longArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement.Substring(idx + paramReference.Length);

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is List<int> intList)
                    {
                        var newParamText = string.Join(",", intList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement.Substring(idx + paramReference.Length);

                        Parameters.Remove(kvp.Key);
                    }
                    else if (kvp.Value is List<long> longList)
                    {
                        var newParamText = string.Join(",", longList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                        sqlStatement = sqlStatement.Substring(0, idx) + newParamText + sqlStatement.Substring(idx + paramReference.Length);

                        Parameters.Remove(kvp.Key);
                    }
                }

                if (Parameters.Count == 0)
                    Parameters = null;
            }

            return sqlStatement;
        }
    }
}