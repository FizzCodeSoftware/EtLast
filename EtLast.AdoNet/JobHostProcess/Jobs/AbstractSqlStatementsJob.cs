namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementsJob : AbstractJob
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
            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, Process, this, null);
                try
                {
                    lock (connection.Lock)
                    {
                        var statements = CreateSqlStatements(ConnectionString, connection.Connection);

                        foreach (var statement in statements)
                        {
                            AdoNetSqlStatementDebugEventListener.GenerateEvent(Process, () => new AdoNetSqlStatementDebugEvent()
                            {
                                Job = this,
                                ConnectionString = ConnectionString,
                                SqlStatement = statement,
                            });
                        }

                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;

                            for (var i = 0; i < statements.Count; i++)
                            {
                                cmd.CommandText = statements[i];
                                try
                                {
                                    RunCommand(cmd, i, startedOn);
                                }
                                catch (Exception)
                                {
                                    LogSucceeded(i - 1, startedOn);
                                    throw;
                                }
                            }

                            LogSucceeded(statements.Count - 1, startedOn);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(Process, this, null, ref connection);
                }
            }
        }

        protected abstract void Validate();

        protected abstract List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection);

        protected abstract void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn);
        protected abstract void LogSucceeded(int lastSucceededIndex, Stopwatch startedOn);
    }
}