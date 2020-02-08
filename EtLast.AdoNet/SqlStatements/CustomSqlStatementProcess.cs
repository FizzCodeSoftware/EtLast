namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

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

        public CustomSqlStatementProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(SqlStatement))
                throw new ProcessParameterNullException(this, nameof(SqlStatement));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters)
        {
            var sqlStatementProcessed = InlineArrayParametersIfNecessary(SqlStatement);
            return sqlStatementProcessed;
        }

        protected override void RunCommand(IDbCommand command)
        {
            Context.Log(LogSeverity.Debug, this, "executing custom SQL statement {SqlStatement} on {ConnectionStringName}, timeout: {Timeout} sec, transaction: {Transaction}", command.CommandText,
                ConnectionString.Name, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            if (Parameters != null)
            {
                foreach (var kvp in Parameters)
                {
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = kvp.Key;
                    parameter.Value = kvp.Value;
                    command.Parameters.Add(parameter);
                }
            }

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.Log(LogSeverity.Information, this, "custom SQL statement affected {RecordCount} records in {Elapsed}, transaction: {Transaction}", recordCount,
                    LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "custom SQL statement failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "custom SQL statement failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                    ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocation.Elapsed);
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