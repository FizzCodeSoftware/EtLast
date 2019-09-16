﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;

    public class CustomSqlStatementJob : AbstractSqlStatementJob
    {
        public string SqlStatement { get; set; }
        public Dictionary<string, object> Parameters { get; set; }

        /// <summary>
        /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
        /// If set to true, then all int[], long[], List<int> and List<long> parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
        /// Default value is true.
        /// </summary>
        public bool InlineArrayParameters { get; set; } = true;

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(SqlStatement))
                throw new JobParameterNullException(process, this, nameof(SqlStatement));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            var sqlStatementProcessed = InlineArrayParametersIfNecessary(SqlStatement);
            return sqlStatementProcessed;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "executing custom SQL statement {SqlStatement} on {ConnectionStringKey}, timeout: {Timeout} sec, transaction: {Transaction}",
                command.CommandText, ConnectionStringSettings.Name, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

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
                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records affected in {Elapsed}", recordCount, startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "custom SQL statement failed", ex);
                exception.AddOpsMessage(string.Format("custom SQL statement failed, connection string key: {0}, message {1}, command: {2}, timeout: {3}",
                    ConnectionStringSettings.Name, ex.Message, SqlStatement, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("Statement", SqlStatement);
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