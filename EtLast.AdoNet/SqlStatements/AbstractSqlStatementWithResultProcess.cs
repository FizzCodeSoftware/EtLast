namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public abstract class AbstractSqlStatementWithResultProcess<T> : AbstractProcess
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 300;

        protected AbstractSqlStatementWithResultProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
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

        public T Execute(IProcess caller = null)
        {
            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            Validate();

            if (Context.CancellationTokenSource.IsCancellationRequested)
                return default;

            try
            {
                return ExecuteImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return default;
        }

        private T ExecuteImpl()
        {
            var parameters = new Dictionary<string, object>();
            var sqlStatement = CreateSqlStatement(ConnectionString, parameters);

            Context.LogDataStoreCommand(ConnectionString.Name, this, sqlStatement, parameters);

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