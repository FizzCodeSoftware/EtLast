namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    public class CopyTableIntoNewTableJob : AbstractSqlStatementJob
    {
        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }

        /// <summary>
        /// Optional. In case of NULL all columns will be copied to the target table.
        /// </summary>
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
        /// </summary>
        public string WhereClause { get; set; } = null;

        protected override void Validate(IProcess process)
        {
            if (string.IsNullOrEmpty(SourceTableName))
                throw new JobParameterNullException(process, this, nameof(SourceTableName));
            if (string.IsNullOrEmpty(TargetTableName))
                throw new JobParameterNullException(process, this, nameof(TargetTableName));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            var columnList = (ColumnConfiguration == null || ColumnConfiguration.Count == 0)
                 ? "*"
                 : string.Join(", ", ColumnConfiguration.Select(x => x.FromColumn + " AS " + x.ToColumn));

            var statement = "DROP TABLE IF EXISTS " + TargetTableName + "; SELECT " + columnList + " INTO " + TargetTableName + " FROM " + SourceTableName;

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            return statement;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "creating new table {ConnectionStringKey}/{TargetTableName} and copying records from {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, TargetTableName, SourceTableName, command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                var recordCount = command.ExecuteNonQuery();
                process.Context.Log(LogSeverity.Information, process, "table {ConnectionStringKey}/{TargetTableName} created and {RecordCount} records copied from {SourceTableName} in {Elapsed}",
                    ConnectionStringSettings.Name, TargetTableName, recordCount, SourceTableName, startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database table creation and copy failed", ex);
                exception.AddOpsMessage(string.Format("database table creation and copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message {4}, command: {5}, timeout: {6}",
                    ConnectionStringSettings.Name, SourceTableName, TargetTableName, ColumnConfiguration != null ? string.Join(",", ColumnConfiguration.Select(x => x.FromColumn)) : "all", ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("SourceTableName", SourceTableName);
                exception.Data.Add("TargetTableName", TargetTableName);
                if (ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", ColumnConfiguration.Select(x => x.FromColumn)));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}