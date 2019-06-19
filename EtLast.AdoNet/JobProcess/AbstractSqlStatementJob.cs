namespace FizzCode.EtLast.AdoNet
{
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public abstract class AbstractSqlStatementJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 300;

        /// <summary>
        /// If true, this job will execute out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; } = false;

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new JobParameterNullException(process, this, nameof(ConnectionStringKey));

            Validate(process);

            var sw = Stopwatch.StartNew();
            var connectionStringSettings = process.Context.GetConnectionStringSettings(ConnectionStringKey);
            var statement = CreateSqlStatement(process, connectionStringSettings);

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

                            RunCommand(process, cmd, sw);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(ref connection);
                }
            }
        }

        protected abstract void Validate(IProcess process);

        protected abstract string CreateSqlStatement(IProcess process, ConnectionStringSettings settings);

        protected abstract void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn);
    }
}