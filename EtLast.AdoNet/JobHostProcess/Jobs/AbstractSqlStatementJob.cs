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

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new JobParameterNullException(process, this, nameof(ConnectionStringKey));

            Validate(process);

            var startedOn = Stopwatch.StartNew();
            ConnectionString = process.Context.GetConnectionString(ConnectionStringKey);
            var statement = CreateSqlStatement(process, ConnectionString);

            AdoNetSqlStatementDebugEventListener.GenerateEvent(process, () => new AdoNetSqlStatementDebugEvent()
            {
                Job = this,
                ConnectionString = ConnectionString,
                SqlStatement = statement,
            });

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, process);
                try
                {
                    lock (connection.Lock)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;
                            cmd.CommandText = statement;

                            RunCommand(process, cmd, startedOn);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(process, ref connection);
                }
            }
        }

        protected abstract void Validate(IProcess process);

        protected abstract string CreateSqlStatement(IProcess process, ConnectionStringWithProvider connectionString);

        protected abstract void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn);
    }
}