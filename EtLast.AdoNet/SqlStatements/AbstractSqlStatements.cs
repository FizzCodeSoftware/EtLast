namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatements : AbstractExecutable
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 300;

        protected AbstractSqlStatements(ITopic topic, string name)
            : base(topic, name)
        {
        }

        /// <summary>
        /// If true, this statement will be executed out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        protected override void ValidateImpl()
        {
            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));
        }

        protected override void ExecuteImpl()
        {
            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, this);
                try
                {
                    lock (connection.Lock)
                    {
                        var transactionId = Transaction.Current.ToIdentifierString();

                        // todo: support returning parameters
                        var sqlStatements = CreateSqlStatements(ConnectionString, connection.Connection, transactionId);
                        if (sqlStatements.Count > 0)
                        {
                            using (var cmd = connection.Connection.CreateCommand())
                            {
                                cmd.CommandTimeout = CommandTimeout;

                                var startedOn = Stopwatch.StartNew();

                                for (var i = 0; i < sqlStatements.Count; i++)
                                {
                                    var sqlStatement = sqlStatements[i];

                                    cmd.CommandText = sqlStatement;
                                    try
                                    {
                                        startedOn.Restart();
                                        RunCommand(cmd, i, startedOn, transactionId);
                                    }
                                    catch (Exception)
                                    {
                                        LogSucceeded(i - 1, transactionId);
                                        throw;
                                    }
                                }

                                LogSucceeded(sqlStatements.Count - 1, transactionId);
                            }
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(this, ref connection);
                }
            }
        }

        protected abstract List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId);
        protected abstract void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId);
        protected abstract void LogSucceeded(int lastSucceededIndex, string transactionId);
    }
}