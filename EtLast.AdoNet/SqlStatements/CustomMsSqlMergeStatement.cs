namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public sealed class CustomMsSqlMergeStatement : AbstractSqlStatement
    {
        public string SourceTableName { get; init; }
        public string SourceTableAlias { get; init; }
        public string TargetTableName { get; init; }
        public string TargetTableAlias { get; init; }
        public string OnCondition { get; init; }
        public string WhenMatchedCondition { get; init; }
        public string WhenMatchedAction { get; init; }
        public string WhenNotMatchedByTargetCondition { get; init; }
        public string WhenNotMatchedByTargetAction { get; init; }
        public string WhenNotMatchedBySourceCondition { get; init; }
        public string WhenNotMatchedBySourceAction { get; init; }
        public Dictionary<string, object> Parameters { get; init; }

        /// <summary>
        /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
        /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
        /// Default value is true.
        /// </summary>
        public bool InlineArrayParameters { get; init; } = true;

        public CustomMsSqlMergeStatement(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(SourceTableName))
                throw new ProcessParameterNullException(this, nameof(SourceTableName));

            if (string.IsNullOrEmpty(TargetTableName))
                throw new ProcessParameterNullException(this, nameof(TargetTableName));
        }

        protected override string CreateSqlStatement(Dictionary<string, object> parameters)
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

            sb.Append(';');

            var sqlStatementProcessed = InlineArrayParametersIfNecessary(sb.ToString());

            if (Parameters != null)
            {
                foreach (var p in Parameters)
                    parameters.Add(p.Key, p.Value);
            }

            return sqlStatementProcessed;
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteMerge, ConnectionString.Name, ConnectionString.Unescape(TargetTableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                "merging to {ConnectionStringName}/{TargetTableName} from {SourceTableName}",
                ConnectionString.Name, ConnectionString.Unescape(TargetTableName), ConnectionString.Unescape(SourceTableName));

            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteMerge, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteMerge, iocUid, null, ex);

                var exception = new ProcessExecutionException(this, "custom merge statement failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "custom merge statement failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                    ConnectionString.Name, ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
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