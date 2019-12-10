namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementsProcess : AbstractExecutableProcess
    {
        public string ConnectionStringKey { get; set; }
        protected ConnectionStringWithProvider ConnectionString { get; private set; }
        public int CommandTimeout { get; set; } = 300;

        protected AbstractSqlStatementsProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        /// <summary>
        /// If true, this statement will be executed out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        public override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new ProcessParameterNullException(this, nameof(ConnectionStringKey));
        }

        protected override void ExecuteImpl()
        {
            ConnectionString = Context.GetConnectionString(ConnectionStringKey);
            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, this, null);
                try
                {
                    lock (connection.Lock)
                    {
                        var statements = CreateSqlStatements(ConnectionString, connection.Connection);

                        foreach (var statement in statements)
                        {
                            AdoNetSqlStatementDebugEventListener.GenerateEvent(this, () => new AdoNetSqlStatementDebugEvent()
                            {
                                Process = this,
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
                                    RunCommand(cmd, i);
                                }
                                catch (Exception)
                                {
                                    LogSucceeded(i - 1);
                                    throw;
                                }
                            }

                            LogSucceeded(statements.Count - 1);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(this, null, ref connection);
                }
            }
        }

        protected abstract List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection);

        protected abstract void RunCommand(IDbCommand command, int statementIndex);
        protected abstract void LogSucceeded(int lastSucceededIndex);
    }
}