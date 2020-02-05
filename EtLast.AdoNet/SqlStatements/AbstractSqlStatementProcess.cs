namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementProcess : AbstractExecutableProcess
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
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

        public override void ValidateImpl()
        {
            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));
        }

        protected override void ExecuteImpl()
        {
            var parameters = new Dictionary<string, object>();
            var sqlStatement = CreateSqlStatement(ConnectionString, parameters);

            Context.LogDataStoreCommand(ConnectionString.Name, this, null, sqlStatement, parameters);

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
                            cmd.CommandText = sqlStatement;

                            foreach (var kvp in parameters)
                            {
                                var parameter = cmd.CreateParameter();
                                parameter.ParameterName = kvp.Key;
                                parameter.Value = kvp.Value;
                                cmd.Parameters.Add(parameter);
                            }

                            RunCommand(cmd);
                        }
                    }
                }
                finally
                {
                    ConnectionManager.ReleaseConnection(this, null, ref connection);
                }
            }
        }

        protected abstract string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters);

        protected abstract void RunCommand(IDbCommand command);
    }
}