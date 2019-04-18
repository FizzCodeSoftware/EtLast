namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class CopyTableStructureJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 300;
        public bool SuppressExistingTransactionScope { get; set; } = false;

        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }

        /// <summary>
        /// Optional. In case of NULL all columns will be available in the target table.
        /// </summary>
        public List<(string SourceColumn, string TargetColumn)> ColumnMap { get; set; }

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

                            process.Context.Log(LogSeverity.Debug, process, "creating new table {ConnectionStringKey}/{TargetTableName} based on {SourceTableName} with query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, TargetTableName, SourceTableName, cmd.CommandText, cmd.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                            try
                            {
                                cmd.ExecuteNonQuery();
                                process.Context.Log(LogSeverity.Information, process, "table {ConnectionStringKey}/{TargetTableName} created from {SourceTableName} in {Elapsed}", ConnectionStringKey, TargetTableName, SourceTableName, sw.Elapsed);
                            }
                            catch (Exception ex)
                            {
                                var exception = new JobExecutionException(process, this, "database table structure copy failed", ex);
                                exception.AddOpsMessage(string.Format("database table structure copy failed, connection string key: {0}, source table: {1}, target table: {2}, columns: {3}, message {4}, command: {5}, timeout: {6}", ConnectionStringKey, SourceTableName, TargetTableName, (ColumnMap != null ? string.Join(",", ColumnMap.Select(x => x.SourceColumn)) : "all"), ex.Message, statement, CommandTimeout));
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
            var columnList = (ColumnMap == null || ColumnMap.Count == 0)
                ? "*"
                : string.Join(", ", ColumnMap.Select(x => x.SourceColumn + " AS " + x.TargetColumn));

            var statement = "DROP TABLE IF EXISTS " + TargetTableName + "; SELECT " + columnList + " INTO " + TargetTableName + " FROM " + SourceTableName;
            statement += " WHERE 1=0";

            return statement;
        }
    }
}