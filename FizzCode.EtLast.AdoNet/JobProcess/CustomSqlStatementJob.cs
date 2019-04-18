namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public class CustomSqlStatementJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 300;
        public bool SuppressExistingTransactionScope { get; set; } = false;
        public string SqlStatement { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new InvalidJobParameterException(process, this, nameof(ConnectionStringKey), ConnectionStringKey, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(SqlStatement)) throw new InvalidJobParameterException(process, this, nameof(SqlStatement), SqlStatement, InvalidOperationParameterException.ValueCannotBeNullMessage);

            var sw = Stopwatch.StartNew();
            var connectionStringSettings = process.Context.GetConnectionStringSettings(ConnectionStringKey);

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
                            cmd.CommandText = SqlStatement;

                            process.Context.Log(LogSeverity.Debug, process, "executing custom sql statement {SqlStatement} on {ConnectionStringKey}, timeout: {Timeout} sec, transaction: {Transaction}", cmd.CommandText, ConnectionStringKey, cmd.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                            try
                            {
                                var recordCount = cmd.ExecuteNonQuery();
                                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records affected in {Elapsed}", recordCount, sw.Elapsed);
                            }
                            catch (Exception ex)
                            {
                                var exception = new JobExecutionException(process, this, "custom sql statement failed", ex);
                                exception.AddOpsMessage(string.Format("custom sql statement failed, connection string key: {0}, message {1}, command: {2}, timeout: {3}", ConnectionStringKey, ex.Message, SqlStatement, CommandTimeout));
                                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                                exception.Data.Add("Statement", SqlStatement);
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
    }
}