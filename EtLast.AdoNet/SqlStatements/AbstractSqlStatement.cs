namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;

    public abstract class AbstractSqlStatement : AbstractSqlStatementBase
    {
        protected AbstractSqlStatement(ITopic topic, string name)
            : base(topic, name)
        {
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