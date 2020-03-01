namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
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

            var netTimeStopwatch = Stopwatch.StartNew();
            try
            {
                try
                {
                    ValidateImpl();
                }
                catch (EtlException ex)
                {
                    Context.AddException(this, ex);
                    return default;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, ex));
                    return default;
                }

                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return default;

                try
                {
                    return ExecuteImpl();
                }
                catch (EtlException ex) { Context.AddException(this, ex); }
                catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
            }
            finally
            {
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
            }

            return default;
        }

        protected virtual void ValidateImpl()
        {
            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));
        }

        private T ExecuteImpl()
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

                            var transactionId = Transaction.Current.ToIdentifierString();
                            LogAction(transactionId);

                            foreach (var kvp in parameters)
                            {
                                var parameter = cmd.CreateParameter();
                                parameter.ParameterName = kvp.Key;
                                parameter.Value = kvp.Value;
                                cmd.Parameters.Add(parameter);
                            }

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

        protected abstract void LogAction(string transactionId);
        protected abstract string CreateSqlStatement(Dictionary<string, object> parameters);
        protected abstract T RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters);
    }
}