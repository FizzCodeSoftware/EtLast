namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public static class ConnectionManager
    {
        private static readonly Dictionary<string, DatabaseConnection> Connections = new Dictionary<string, DatabaseConnection>();

        internal static DatabaseConnection GetConnection(ConnectionStringWithProvider connectionString, IProcess process, IBaseOperation operation, int maxRetryCount = 5, int retryDelayMilliseconds = 2000)
        {
            if (string.IsNullOrEmpty(connectionString.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringKey"] = connectionString.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionString.Name);
                throw ex;
            }

            var key = connectionString.Name;

            if (Transaction.Current != null)
            {
                key += Transaction.Current.TransactionInformation.CreationTime.Ticks.ToString("D", CultureInfo.InvariantCulture);
            }
            else
            {
                key += "-";
            }

            Exception lastException = null;

            for (var retry = 0; retry <= maxRetryCount; retry++)
            {
                lock (Connections)
                {
                    if (Connections.TryGetValue(key, out var connection))
                    {
                        connection.ReferenceCount++;
                        return connection;
                    }

                    var startedOn = Stopwatch.StartNew();

                    process.Context.Log(LogSeverity.Debug, process, operation, "opening database connection to {ConnectionStringKey} ({Provider}), transaction: {Transaction}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), Transaction.Current.ToIdentifierString());

                    try
                    {
                        IDbConnection conn = null;

                        var connectionType = Type.GetType(connectionString.ProviderName);
                        if (connectionType != null)
                        {
                            conn = Activator.CreateInstance(connectionType) as IDbConnection;
                        }

                        if (conn == null)
                        {
                            conn = DbProviderFactories.GetFactory(connectionString.ProviderName).CreateConnection();
                        }

                        process.CounterCollection.IncrementCounter("db connections opened", 1);
                        process.Context.CounterCollection.IncrementCounter("db connections opened / " + connectionString.Name, 1);

                        conn.ConnectionString = connectionString.ConnectionString;
                        conn.Open();

                        process.Context.Log(LogSeverity.Debug, process, operation, "database connection opened to {ConnectionStringKey} ({Provider}) in {Elapsed}", connectionString.Name,
                            connectionString.GetFriendlyProviderName(), startedOn.Elapsed);

                        connection = new DatabaseConnection()
                        {
                            Key = key,
                            ConnectionString = connectionString,
                            Connection = conn,
                            ReferenceCount = 1,
                            TransactionWhenCreated = Transaction.Current,
                        };

                        Connections.Add(key, connection);

                        return connection;
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                    }
                } // lock released

                process.CounterCollection.IncrementCounter("db connections failed", 1);
                process.Context.CounterCollection.IncrementCounter("db connections failed / " + connectionString.Name, 1);

                if (retry < maxRetryCount)
                {
                    process.Context.Log(LogSeverity.Error, process, operation, "can't connect to database, connection string key: {ConnectionStringKey} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retry + 1), retry, lastException.Message);

                    process.Context.LogOps(LogSeverity.Error, process, operation, "can't connect to database, connection string key: {ConnectionStringKey} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retry + 1), retry, lastException.Message);

                    Thread.Sleep(retryDelayMilliseconds * (retry + 1));
                }
            }

            var exception = new EtlException(process, "can't connect to database", lastException);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't connect to database, connection string key: {0}, message: {1}", connectionString.Name, lastException.Message));
            exception.Data.Add("ConnectionStringKey", connectionString.Name);
            exception.Data.Add("ProviderName", connectionString.ProviderName);
            exception.Data.Add("NumberOfAttempts", maxRetryCount + 1);
            throw exception;
        }

        public static DatabaseConnection GetNewConnection(ConnectionStringWithProvider connectionString, IProcess process, IBaseOperation operation, int maxRetryCount = 5, int retryDelayMilliseconds = 2000)
        {
            if (string.IsNullOrEmpty(connectionString.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringKey"] = connectionString.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionString.Name);
                throw ex;
            }

            Exception lastException = null;

            for (var retry = 0; retry <= maxRetryCount; retry++)
            {
                var startedOn = Stopwatch.StartNew();
                process.Context.Log(LogSeverity.Debug, process, operation, "opening database connection to {ConnectionStringKey} ({Provider}), transaction: {Transaction}", connectionString.Name,
                    connectionString.GetFriendlyProviderName(), Transaction.Current.ToIdentifierString());

                try
                {
                    IDbConnection conn = null;

                    var connectionType = Type.GetType(connectionString.ProviderName);
                    if (connectionType != null)
                    {
                        conn = Activator.CreateInstance(connectionType) as IDbConnection;
                    }

                    if (conn == null)
                    {
                        conn = DbProviderFactories.GetFactory(connectionString.ProviderName).CreateConnection();
                    }

                    process.CounterCollection.IncrementCounter("db connections opened", 1);
                    process.Context.CounterCollection.IncrementCounter("db connections opened / " + connectionString.Name, 1);

                    conn.ConnectionString = connectionString.ConnectionString;
                    conn.Open();

                    process.Context.Log(LogSeverity.Debug, process, operation, "database connection opened to {ConnectionStringKey} ({Provider}) in {Elapsed}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), startedOn.Elapsed);

                    return new DatabaseConnection()
                    {
                        Key = null,
                        ConnectionString = connectionString,
                        Connection = conn,
                        ReferenceCount = 1,
                        TransactionWhenCreated = Transaction.Current,
                    };
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }

                process.CounterCollection.IncrementCounter("db connections opened", 1);
                process.Context.CounterCollection.IncrementCounter("db connections opened / " + connectionString.Name, 1);

                if (retry < maxRetryCount)
                {
                    process.Context.Log(LogSeverity.Error, process, operation, "can't connect to database, connection string key: {ConnectionStringKey} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retry + 1), retry, lastException.Message);

                    process.Context.LogOps(LogSeverity.Error, process, operation, "can't connect to database, connection string key: {ConnectionStringKey} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionString.Name,
                        connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retry + 1), retry, lastException.Message);

                    Thread.Sleep(retryDelayMilliseconds * (retry + 1));
                }
            }

            var exception = new EtlException(process, "can't connect to database", lastException);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't connect to database, connection string key: {0}, message: {1}", connectionString.Name, lastException.Message));
            exception.Data.Add("ConnectionStringKey", connectionString.Name);
            exception.Data.Add("ProviderName", connectionString.ProviderName);
            throw exception;
        }

        public static void ReleaseConnection(IProcess process, IBaseOperation operation, ref DatabaseConnection connection)
        {
            if (connection == null)
                return;

            lock (Connections)
            {
                connection.ReferenceCount--;

                if (connection.ReferenceCount == 0)
                {
                    if (connection.Key != null)
                    {
                        Connections.Remove(connection.Key);
                    }

                    process.Context.Log(LogSeverity.Debug, process, operation, "database connection closed: {ConnectionStringKey} ({Provider})", connection.ConnectionString.Name,
                        connection.ConnectionString.GetFriendlyProviderName());

                    if (connection != null)
                    {
                        connection.Connection.Close();
                        connection.Connection.Dispose();
                    }
                }
                else
                {
                    process.Context.Log(LogSeverity.Debug, process, operation, "database connection reference count decreased to {ReferenceCount}: {ConnectionStringKey} ({Provider})", connection.ReferenceCount,
                        connection.ConnectionString.Name, connection.ConnectionString.GetFriendlyProviderName());
                }
            }

            connection = null;
        }

        public static void ConnectionFailed(ref DatabaseConnection connection)
        {
            lock (Connections)
            {
                connection.ReferenceCount--;
                connection.Failed = true;

                if (connection.Key != null)
                {
                    Connections.Remove(connection.Key);
                }

                if (connection.ReferenceCount == 0)
                {
                    if (connection != null)
                    {
                        connection.Connection.Close();
                        connection.Connection.Dispose();
                    }
                }
            }

            connection = null;
        }

        public static void TestConnection(ConnectionStringWithProvider connectionString)
        {
            if (string.IsNullOrEmpty(connectionString.ProviderName))
                throw new Exception("missing provider name for connection string");

            IDbConnection conn = null;

            var connectionType = Type.GetType(connectionString.ProviderName);
            if (connectionType != null)
            {
                conn = Activator.CreateInstance(connectionType) as IDbConnection;
            }

            if (conn == null)
            {
                conn = DbProviderFactories.GetFactory(connectionString.ProviderName).CreateConnection();
            }

            conn.ConnectionString = connectionString.ConnectionString;
            conn.Open();

            conn.Close();
            conn.Dispose();
        }
    }
}