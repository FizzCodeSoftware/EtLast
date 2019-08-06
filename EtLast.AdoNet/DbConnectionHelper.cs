namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public static class ConnectionManager
    {
        private static readonly Dictionary<string, DatabaseConnection> Connections = new Dictionary<string, DatabaseConnection>();

        internal static DatabaseConnection GetConnection(ConnectionStringSettings connectionStringSettings, IProcess process, bool ignorePool = false, int maxRetryCount = 1, int retryDelayMilliseconds = 10000)
        {
            if (string.IsNullOrEmpty(connectionStringSettings.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringKey"] = connectionStringSettings.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionStringSettings.Name);
                throw ex;
            }

            var key = connectionStringSettings.Name + "/" + connectionStringSettings.ProviderName + "/";

            key += Transaction.Current != null
                ? Transaction.Current.TransactionInformation.CreationTime.ToString()
                : "-";

            if (ignorePool)
            {
                key += Guid.NewGuid().ToString();
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
                    process.Context.Log(LogSeverity.Debug, process, "opening database connection to {ConnectionStringKey} using {ProviderName} provider, transaction: {Transaction}", connectionStringSettings.Name, connectionStringSettings.ProviderName, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                    try
                    {
                        IDbConnection conn = null;

                        var providerName = connectionStringSettings.ProviderName;
                        if (providerName != null)
                        {
                            var connectionType = Type.GetType(providerName);
                            if (connectionType != null)
                            {
                                conn = Activator.CreateInstance(connectionType) as IDbConnection;
                            }
                        }

                        if (conn == null)
                        {
                            conn = DbProviderFactories.GetFactory(providerName).CreateConnection();
                        }

                        process.Context.Stat.IncrementCounter("database connections opened", 1);
                        process.Context.Stat.IncrementCounter("database connections opened / " + connectionStringSettings.Name, 1);

                        conn.ConnectionString = connectionStringSettings.ConnectionString;
                        conn.Open();

                        process.Context.Log(LogSeverity.Debug, process, "database connection opened to {ConnectionStringKey} using {ProviderName} provider in {Elapsed}", connectionStringSettings.Name, connectionStringSettings.ProviderName, startedOn.Elapsed);

                        connection = new DatabaseConnection()
                        {
                            Key = key,
                            Settings = connectionStringSettings,
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

                process.Context.Stat.IncrementCounter("database connections failed / " + connectionStringSettings.Name, 1);

                process.Context.Log(LogSeverity.Information, process, "can't connect to database, connection string key: {ConnectionStringKey} using {ProviderName} provider, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionStringSettings.Name, connectionStringSettings.ProviderName, retryDelayMilliseconds * (retry + 1), retry, lastException.Message);
                process.Context.LogOps(LogSeverity.Information, process, "can't connect to database, connection string key: {ConnectionStringKey} using {ProviderName} provider, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionStringSettings.Name, connectionStringSettings.ProviderName, retryDelayMilliseconds * (retry + 1), retry, lastException.Message);
                Thread.Sleep(retryDelayMilliseconds * (retry + 1));
            }

            var exception = new EtlException(process, "can't connect to database", lastException);
            exception.AddOpsMessage(string.Format("can't connect to database, connection string key: {0}, message: {1}", connectionStringSettings.Name, lastException.Message));
            exception.Data.Add("ConnectionStringKey", connectionStringSettings.Name);
            exception.Data.Add("ProviderName", connectionStringSettings.ProviderName);
            throw exception;
        }

        public static DatabaseConnection GetNewConnection(ConnectionStringSettings connectionStringSettings, IProcess process, int maxRetryCount = 1, int retryDelayMilliseconds = 10000)
        {
            if (string.IsNullOrEmpty(connectionStringSettings.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringKey"] = connectionStringSettings.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionStringSettings.Name);
                throw ex;
            }

            Exception lastException = null;

            for (var retry = 0; retry <= maxRetryCount; retry++)
            {
                var startedOn = Stopwatch.StartNew();
                process.Context.Log(LogSeverity.Debug, process, "opening database connection to {ConnectionStringKey} using {ProviderName} provider, transaction: {Transaction}", connectionStringSettings.Name, connectionStringSettings.ProviderName, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                try
                {
                    IDbConnection conn = null;

                    var providerName = connectionStringSettings.ProviderName;
                    if (providerName != null)
                    {
                        var connectionType = Type.GetType(providerName);
                        if (connectionType != null)
                        {
                            conn = Activator.CreateInstance(connectionType) as IDbConnection;
                        }
                    }

                    if (conn == null)
                    {
                        conn = DbProviderFactories.GetFactory(providerName).CreateConnection();
                    }

                    process.Context.Stat.IncrementCounter("database connections opened", 1);
                    process.Context.Stat.IncrementCounter("database connections opened / " + connectionStringSettings.Name, 1);

                    conn.ConnectionString = connectionStringSettings.ConnectionString;
                    conn.Open();

                    process.Context.Log(LogSeverity.Debug, process, "database connection opened to {ConnectionStringKey} using {ProviderName} provider in {Elapsed}", connectionStringSettings.Name, connectionStringSettings.ProviderName, startedOn.Elapsed);

                    return new DatabaseConnection()
                    {
                        Key = null,
                        Settings = connectionStringSettings,
                        Connection = conn,
                        ReferenceCount = 1,
                        TransactionWhenCreated = Transaction.Current,
                    };
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }

                process.Context.Stat.IncrementCounter("database connections failed / " + connectionStringSettings.Name, 1);

                process.Context.Log(LogSeverity.Information, process, "can't connect to database, connection string key: {ConnectionStringKey} using {ProviderName} provider, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionStringSettings.Name, connectionStringSettings.ProviderName, retryDelayMilliseconds * (retry + 1), retry, lastException.Message);
                process.Context.LogOps(LogSeverity.Information, process, "can't connect to database, connection string key: {ConnectionStringKey} using {ProviderName} provider, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", connectionStringSettings.Name, connectionStringSettings.ProviderName, retryDelayMilliseconds * (retry + 1), retry, lastException.Message);
                Thread.Sleep(retryDelayMilliseconds * (retry + 1));
            }

            var exception = new EtlException(process, "can't connect to database", lastException);
            exception.AddOpsMessage(string.Format("can't connect to database, connection string key: {0}, message: {1}", connectionStringSettings.Name, lastException.Message));
            exception.Data.Add("ConnectionStringKey", connectionStringSettings.Name);
            exception.Data.Add("ProviderName", connectionStringSettings.ProviderName);
            throw exception;
        }

        public static void ReleaseConnection(IProcess process, ref DatabaseConnection connection)
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

                    process.Context.Log(LogSeverity.Debug, process, "database connection closed: {ConnectionStringKey} using {ProviderName} provider", connection.Settings.Name, connection.Settings.ProviderName);

                    if (connection != null)
                    {
                        connection.Connection.Close();
                        connection.Connection.Dispose();
                    }
                }
                else
                {
                    process.Context.Log(LogSeverity.Debug, process, "database connection reference count decreased to {ReferenceCount}: {ConnectionStringKey} using {ProviderName} provider", connection.ReferenceCount, connection.Settings.Name, connection.Settings.ProviderName);
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
    }
}