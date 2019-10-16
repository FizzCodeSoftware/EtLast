namespace FizzCode.EtLast.AdoNet
{
    using System.Data;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        protected ConnectionStringWithProvider ConnectionString { get; private set; }
        public int CommandTimeout { get; set; } = 300;

        /// <summary>
        /// If true, this job will execute out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        public override void Execute(CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new JobParameterNullException(Process, this, nameof(ConnectionStringKey));

            Validate();

            var startedOn = Stopwatch.StartNew();
            ConnectionString = Process.Context.GetConnectionString(ConnectionStringKey);
            var statement = CreateSqlStatement(ConnectionString);

            AdoNetSqlStatementDebugEventListener.GenerateEvent(Process, () => new AdoNetSqlStatementDebugEvent()
            {
                Job = this,
                ConnectionString = ConnectionString,
                SqlStatement = statement,
            });

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, Process);
                try
                {
                    lock (connection.Lock)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;
                            cmd.CommandText = statement;

                            RunCommand(cmd, startedOn);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(Process, ref connection);
                }
            }
        }

        protected abstract void Validate();

        protected abstract string CreateSqlStatement(ConnectionStringWithProvider connectionString);

        protected abstract void RunCommand(IDbCommand command, Stopwatch startedOn);
    }
}