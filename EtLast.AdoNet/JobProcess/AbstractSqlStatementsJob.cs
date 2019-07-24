namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public abstract class AbstractSqlStatementsJob : AbstractJob
    {
        public string ConnectionStringKey { get; set; }
        protected ConnectionStringSettings ConnectionStringSettings { get; private set; }
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

            var sw = Stopwatch.StartNew();
            ConnectionStringSettings = process.Context.GetConnectionStringSettings(ConnectionStringKey);
            var statements = CreateSqlStatements(process, ConnectionStringSettings);

            foreach (var statement in statements)
            {
                AdoNetSqlStatementDebugEventListener.GenerateEvent(process, () => new AdoNetSqlStatementDebugEvent()
                {
                    Job = this,
                    ConnectionStringSettings = ConnectionStringSettings,
                    SqlStatement = statement,
                });
            }

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionStringSettings, process);
                try
                {
                    lock (connection.Lock)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;

                            for (var i = 0; i < statements.Count; i++)
                            {
                                cmd.CommandText = statements[i];
                                RunCommand(process, cmd, i, sw);
                            }
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

        protected abstract List<string> CreateSqlStatements(IProcess process, ConnectionStringSettings settings);

        protected abstract void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn);
    }
}