namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public class DropTableJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 300;
        public bool SuppressExistingTransactionScope { get; set; } = false;

        public string TableName { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new JobParameterNullException(process, this, nameof(ConnectionStringKey));
            if (string.IsNullOrEmpty(TableName)) throw new JobParameterNullException(process, this, nameof(TableName));

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

                            process.Context.Log(LogSeverity.Debug, process, "dropping table {ConnectionStringKey}/{TableName} with query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, TableName, cmd.CommandText, cmd.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                            try
                            {
                                cmd.ExecuteNonQuery();
                                process.Context.Log(LogSeverity.Information, process, "table {ConnectionStringKey}/{TableName} dropped", ConnectionStringKey, TableName);
                            }
                            catch (Exception ex)
                            {
                                var exception = new JobExecutionException(process, this, "database table drop failed", ex);
                                exception.AddOpsMessage(string.Format("database table drop failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}", ConnectionStringKey, TableName, ex.Message, statement, CommandTimeout));
                                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                                exception.Data.Add("TableName", TableName);
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
            return "DROP TABLE IF EXISTS " + TableName;
        }
    }
}