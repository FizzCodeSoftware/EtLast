namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class CopyTableIntoExistingTableJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 300;
        public bool SuppressExistingTransactionScope { get; set; } = false;

        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }
        public bool CopyIdentityColumns { get; set; } = false;

        /// <summary>
        /// Optional. In case of NULL all columns will be available in the target table.
        /// </summary>
        public List<(string SourceColumn, string TargetColumn)> ColumnMap { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the source table to the target table.
        /// </summary>
        public string WhereClause { get; set; } = null;

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new InvalidJobParameterException(process, this, nameof(ConnectionStringKey), ConnectionStringKey, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(SourceTableName)) throw new InvalidJobParameterException(process, this, nameof(SourceTableName), SourceTableName, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(TargetTableName)) throw new InvalidJobParameterException(process, this, nameof(TargetTableName), TargetTableName, InvalidOperationParameterException.ValueCannotBeNullMessage);

            var sw = Stopwatch.StartNew();
            var connectionStringSettings = process.Context.GetConnectionStringSettings(ConnectionStringKey);
            var statement = CreateSqlStatement(connectionStringSettings);

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(connectionStringSettings, process);
                try
                {
                    lock (connection.Lock)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;
                            cmd.CommandText = statement;

                            process.Context.Log(LogSeverity.Debug, process, "copying records from {ConnectionStringKey}/{SourceTableName} to {TargetTableName} with query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, SourceTableName, TargetTableName, cmd.CommandText, cmd.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                            try
                            {
                                var recordCount = cmd.ExecuteNonQuery();
                                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records copied to {ConnectionStringKey}/{TargetTableName} from {SourceTableName} in {Elapsed}", recordCount, ConnectionStringKey, TargetTableName, SourceTableName, sw.Elapsed);
                            }
                            catch (Exception ex)
                            {
                                var exception = new JobExecutionException(process, this, "database table copy failed", ex);
                                exception.AddOpsMessage(string.Format("database table copy failed, connection string key: {0}, source table: {1}, target table: {2}, columns: {3}, message {4}, command: {5}, timeout: {6}", ConnectionStringKey, SourceTableName, TargetTableName, (ColumnMap != null ? string.Join(",", ColumnMap.Select(x => x.SourceColumn)) : "all"), ex.Message, statement, CommandTimeout));
                                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                                exception.Data.Add("SourceTableName", SourceTableName);
                                exception.Data.Add("TargetTableName", TargetTableName);
                                if (ColumnMap != null)
                                {
                                    exception.Data.Add("Columns", string.Join(",", ColumnMap.Select(x => x.SourceColumn)));
                                }

                                exception.Data.Add("Statement", statement);
                                exception.Data.Add("Timeout", CommandTimeout);
                                exception.Data.Add("Elapsed", sw.Elapsed);
                                throw exception;
                            }
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(ref connection);
                }
            }
        }

        protected virtual string CreateSqlStatement(ConnectionStringSettings settings)
        {
            var statement = string.Empty;
            if (CopyIdentityColumns && settings.ProviderName == "System.Data.SqlClient")
            {
                statement = "SET IDENTITY_INSERT " + TargetTableName + " ON; ";
            }

            if (ColumnMap == null || ColumnMap.Count == 0)
            {
                statement += "INSERT INTO " + TargetTableName + " SELECT * FROM " + SourceTableName;
            }
            else
            {
                var sourceColumnList = string.Join(", ", ColumnMap.Select(x => x.SourceColumn));
                var targetColumnList = string.Join(", ", ColumnMap.Select(x => x.TargetColumn));

                statement += "INSERT INTO " + TargetTableName + " (" + targetColumnList + ") SELECT " + sourceColumnList + " FROM " + SourceTableName;
            }

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            if (CopyIdentityColumns && settings.ProviderName == "System.Data.SqlClient")
            {
                statement += "; SET IDENTITY_INSERT " + TargetTableName + " OFF; ";
            }

            return statement;
        }
    }
}