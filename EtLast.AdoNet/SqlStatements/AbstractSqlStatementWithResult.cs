﻿namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractSqlStatementWithResult<T> : AbstractExecutableWithResult<T>
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 300;

        /// <summary>
        /// If true, this statement will be executed out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        protected AbstractSqlStatementWithResult(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));
        }

        protected sealed override T ExecuteImpl()
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
                            var result = RunCommandAndGetResult(cmd, transactionId, parameters);
                            return result;
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
        protected abstract T RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters);
    }
}