namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    public class CopyTableStructureJob : AbstractSqlStatementJob
    {
        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }

        /// <summary>
        /// Optional. In case of NULL all columns will be available in the target table.
        /// </summary>
        public List<(string SourceColumn, string TargetColumn)> ColumnMap { get; set; }

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(SourceTableName))
                throw new JobParameterNullException(process, this, nameof(SourceTableName));
            if (string.IsNullOrEmpty(TargetTableName))
                throw new JobParameterNullException(process, this, nameof(TargetTableName));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            var columnList = (ColumnMap == null || ColumnMap.Count == 0)
                ? "*"
                : string.Join(", ", ColumnMap.Select(x => x.SourceColumn + " AS " + x.TargetColumn));

            var statement = "DROP TABLE IF EXISTS " + TargetTableName + "; SELECT " + columnList + " INTO " + TargetTableName + " FROM " + SourceTableName;
            statement += " WHERE 1=0";

            return statement;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "creating new table {ConnectionStringKey}/{TargetTableName} based on {SourceTableName} with query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringKey, TargetTableName, SourceTableName, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "table {ConnectionStringKey}/{TargetTableName} created from {SourceTableName} in {Elapsed}",
                    ConnectionStringKey, TargetTableName, SourceTableName, startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database table structure copy failed", ex);
                exception.AddOpsMessage(string.Format("database table structure copy failed, connection string key: {0}, source table: {1}, target table: {2}, columns: {3}, message {4}, command: {5}, timeout: {6}",
                    ConnectionStringKey, SourceTableName, TargetTableName, ColumnMap != null ? string.Join(",", ColumnMap.Select(x => x.SourceColumn)) : "all", ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                exception.Data.Add("SourceTableName", SourceTableName);
                exception.Data.Add("TargetTableName", TargetTableName);
                if (ColumnMap != null)
                {
                    exception.Data.Add("Columns", string.Join(",", ColumnMap.Select(x => x.SourceColumn)));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}