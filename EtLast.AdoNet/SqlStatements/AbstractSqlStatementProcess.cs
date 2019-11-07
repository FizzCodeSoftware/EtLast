namespace FizzCode.EtLast.AdoNet
{
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementProcess : AbstractExecutableProcess
    {
        public string ConnectionStringKey { get; set; }
        protected ConnectionStringWithProvider ConnectionString { get; private set; }
        public int CommandTimeout { get; set; } = 300;

        protected AbstractSqlStatementProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        /// <summary>
        /// If true, this statement will be executed out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        public override void Validate()
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new ProcessParameterNullException(this, nameof(ConnectionStringKey));
        }

        protected override void Execute(Stopwatch startedOn)
        {
            ConnectionString = Context.GetConnectionString(ConnectionStringKey);
            var statement = CreateSqlStatement(ConnectionString);

            AdoNetSqlStatementDebugEventListener.GenerateEvent(this, () => new AdoNetSqlStatementDebugEvent()
            {
                Process = this,
                ConnectionString = ConnectionString,
                SqlStatement = statement,
            });

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, this, null);
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
                    ConnectionManager.ReleaseConnection(this, null, ref connection);
                }
            }
        }

        protected abstract string CreateSqlStatement(ConnectionStringWithProvider connectionString);

        protected abstract void RunCommand(IDbCommand command, Stopwatch startedOn);
    }
}