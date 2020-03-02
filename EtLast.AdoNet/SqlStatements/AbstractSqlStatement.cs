namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatement : AbstractExecutable
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 300;

        protected AbstractSqlStatement(ITopic topic, string name)
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
            var parameters = new Dictionary<string, object>();
            var sqlStatement = CreateSqlStatement(parameters);

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                var connection = ConnectionManager.GetConnection(ConnectionString, this);
                try
                {
                    lock (connection.Lock)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;
                            cmd.CommandText = sqlStatement;
                            cmd.FillCommandParameters(parameters);

                            var transactionId = Transaction.Current.ToIdentifierString();
                            RunCommand(cmd, transactionId, parameters);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(this, ref connection);
                }
            }
        }

        protected abstract string CreateSqlStatement(Dictionary<string, object> parameters);
        protected abstract void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters);
    }
}