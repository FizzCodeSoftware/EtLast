namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementWithResultProcess<T> : AbstractProcess, IExecutableWithResult<T>
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 300;

        /// <summary>
        /// If true, this statement will be executed out of ambient transaction scope.
        /// See <see cref="TransactionScopeOption.Suppress"/>>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        protected AbstractSqlStatementWithResultProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public T Execute(IProcess caller = null)
        {
            Context.RegisterProcessInvocationStart(this, caller);

            try
            {
                ValidateImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            if (Context.CancellationTokenSource.IsCancellationRequested)
            {
                Context.RegisterProcessInvocationEnd(this);
                return default;
            }

            T result = default;
            try
            {
                result = ExecuteImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            Context.RegisterProcessInvocationEnd(this);

            return result;
        }

        protected virtual void ValidateImpl()
        {
            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));
        }

        private T ExecuteImpl()
        {
            var parameters = new Dictionary<string, object>();
            var sqlStatement = CreateSqlStatement(ConnectionString, parameters);

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

                            Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.many, ConnectionString.Name, this, sqlStatement, Transaction.Current.ToIdentifierString(), () => parameters);

                            foreach (var kvp in parameters)
                            {
                                var parameter = cmd.CreateParameter();
                                parameter.ParameterName = kvp.Key;
                                parameter.Value = kvp.Value;
                                cmd.Parameters.Add(parameter);
                            }

                            var result = RunCommandAndGetResult(cmd);
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

        protected abstract string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters);

        protected abstract T RunCommandAndGetResult(IDbCommand command);
    }
}