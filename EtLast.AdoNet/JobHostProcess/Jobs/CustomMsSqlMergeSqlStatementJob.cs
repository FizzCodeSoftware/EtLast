namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class CustomMsSqlMergeSqlStatementJob : AbstractSqlStatementJob
    {
        public string SourceTableName { get; set; }
        public string SourceTableAlias { get; set; }
        public string TargetTableName { get; set; }
        public string TargetTableAlias { get; set; }
        public string OnCondition { get; set; }
        public string WhenMatchedCondition { get; set; }
        public string WhenMatchedAction { get; set; }
        public string WhenNotMatchedByTargetCondition { get; set; }
        public string WhenNotMatchedByTargetAction { get; set; }
        public string WhenNotMatchedBySourceCondition { get; set; }
        public string WhenNotMatchedBySourceAction { get; set; }
        public Dictionary<string, object> Parameters { get; set; }

        /// <summary>
        /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
        /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
        /// Default value is true.
        /// </summary>
        public bool InlineArrayParameters { get; set; } = true;

        protected override void Validate()
        {
            if (string.IsNullOrEmpty(SourceTableName))
                throw new JobParameterNullException(Process, this, nameof(SourceTableName));
            if (string.IsNullOrEmpty(TargetTableName))
                throw new JobParameterNullException(Process, this, nameof(TargetTableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString)
        {
            var sb = new StringBuilder();
            sb
                .Append("MERGE INTO ")
                .Append(TargetTableName)
                .Append(!string.IsNullOrEmpty(TargetTableAlias) ? " " + TargetTableAlias : "")
                .Append(" USING ")
                .Append(SourceTableName)
                .Append(!string.IsNullOrEmpty(SourceTableAlias) ? " " + SourceTableAlias : "")
                .Append(" ON ")
                .Append(OnCondition);

            if (!string.IsNullOrEmpty(WhenMatchedAction))
            {
                sb.Append(" WHEN MATCHED");
                if (!string.IsNullOrEmpty(WhenMatchedCondition))
                    sb.Append(" AND ").Append(WhenMatchedCondition);
                sb.Append(" THEN ").Append(WhenMatchedAction);
            }
            if (!string.IsNullOrEmpty(WhenNotMatchedByTargetAction))
            {
                sb.Append(" WHEN NOT MATCHED BY TARGET");
                if (!string.IsNullOrEmpty(WhenNotMatchedByTargetCondition))
                    sb.Append(" AND ").Append(WhenNotMatchedByTargetCondition);
                sb.Append(" THEN ").Append(WhenNotMatchedByTargetAction);
            }
            if (!string.IsNullOrEmpty(WhenNotMatchedBySourceAction))
            {
                sb.Append(" WHEN NOT MATCHED BY SOURCE");
                if (!string.IsNullOrEmpty(WhenNotMatchedBySourceCondition))
                    sb.Append(" AND ").Append(WhenNotMatchedBySourceCondition);
                sb.Append(" THEN ").Append(WhenNotMatchedBySourceAction);
            }

            sb.Append(";");

            var sqlStatementProcessed = InlineArrayParametersIfNecessary(sb.ToString());
            return sqlStatementProcessed;
        }

        protected override void RunCommand(IDbCommand command, Stopwatch startedOn)
        {
            Process.Context.Log(LogSeverity.Debug, Process, this, null, "merging to {ConnectionStringKey}/{TargetTableName} from {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(TargetTableName), Helpers.UnEscapeTableName(SourceTableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

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

                Process.Context.Log(LogSeverity.Information, Process, this, null, "{RecordCount} records merged to {ConnectionStringKey}/{TargetTableName} from {SourceTableName} in {Elapsed}",
                    Name, recordCount, ConnectionString.Name, Helpers.UnEscapeTableName(TargetTableName), Helpers.UnEscapeTableName(SourceTableName), startedOn.Elapsed);

                // todo: support stats in jobs...
                // Stat.IncrementCounter("records merged", recordCount);
                // Stat.IncrementCounter("merge time", startedOn.ElapsedMilliseconds);

                Process.Context.Stat.IncrementCounter("database records merged / " + ConnectionString.Name, recordCount);
                Process.Context.Stat.IncrementDebugCounter("database records merged / " + ConnectionString.Name + " / " + Helpers.UnEscapeTableName(SourceTableName) + " -> " + Helpers.UnEscapeTableName(TargetTableName), recordCount);
                Process.Context.Stat.IncrementCounter("database merge time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
                Process.Context.Stat.IncrementDebugCounter("database merge time / " + ConnectionString.Name + " / " + Helpers.UnEscapeTableName(SourceTableName) + " -> " + Helpers.UnEscapeTableName(TargetTableName), startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "custom merge statement failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "custom merge statement failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                    ConnectionString.Name, ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
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