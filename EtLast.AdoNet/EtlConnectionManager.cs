namespace FizzCode.EtLast.AdoNet
{
    using System.Globalization;
    using System.Transactions;
    using FizzCode.LightWeight.AdoNet;

    public static class EtlConnectionManager
    {
        private static readonly ConnectionManager _connectionManager = new()
        {
            SeparateConnectionsByThreadId = false,
        };

        internal static DatabaseConnection GetConnection(NamedConnectionString connectionString, IProcess process, int maxRetryCount = 5, int retryDelayMilliseconds = 2000)
        {
            if (string.IsNullOrEmpty(connectionString.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringName"] = connectionString.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionString.Name);
                throw ex;
            }

            var iocUid = 0;

            var connection = _connectionManager.GetConnection(connectionString, maxRetryCount, retryDelayMilliseconds,
                onOpening: (connectionString, connection) =>
                {
                    iocUid = process.Context.RegisterIoCommandStart(process, IoCommandKind.dbConnection, connectionString.Name, connection.ConnectionTimeout, "open connection", Transaction.Current.ToIdentifierString(), null,
                        "opening database connection to {ConnectionStringName} ({Provider})", connectionString.Name, connectionString.GetFriendlyProviderName());
                },
                onOpened: (connectionString, connection, retryCount) =>
                {
                    process.Context.RegisterIoCommandSuccess(process, IoCommandKind.dbConnection, iocUid, null);
                },
                onError: (connectionString, connection, retryCount, ex) =>
                {
                    process.Context.RegisterIoCommandFailed(process, IoCommandKind.dbConnection, iocUid, null, ex);

                    if (retryCount < maxRetryCount)
                    {
                        process.Context.Log(LogSeverity.Error, process, "can't connect to database, connection string key: {ConnectionStringName} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                            connectionString.Name, connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retryCount + 1), retryCount, ex.Message);

                        process.Context.LogOps(LogSeverity.Error, process, "can't connect to database, connection string key: {ConnectionStringName} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                            connectionString.Name, connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retryCount + 1), retryCount, ex.Message);
                    }
                    else
                    {
                        var exception = new EtlException(process, "can't connect to database", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't connect to database, connection string key: {0}, message: {1}", connectionString.Name, ex.Message));
                        exception.Data.Add("ConnectionStringName", connectionString.Name);
                        exception.Data.Add("ProviderName", connectionString.ProviderName);
                        exception.Data.Add("NumberOfAttempts", maxRetryCount + 1);
                        throw exception;
                    }
                });

            return connection;
        }

        public static DatabaseConnection GetNewConnection(NamedConnectionString connectionString, IProcess process, int maxRetryCount = 5, int retryDelayMilliseconds = 2000)
        {
            if (string.IsNullOrEmpty(connectionString.ProviderName))
            {
                var ex = new EtlException(process, "missing provider name for connection string");
                ex.Data["ConnectionStringName"] = connectionString.Name;
                ex.AddOpsMessage("missing provider name for connection string key: " + connectionString.Name);
                throw ex;
            }

            var iocUid = 0;

            var connection = _connectionManager.GetNewConnection(connectionString, maxRetryCount, retryDelayMilliseconds,
                onOpening: (connectionString, connection) =>
                {
                    iocUid = process.Context.RegisterIoCommandStart(process, IoCommandKind.dbConnection, connectionString.Name, connection.ConnectionTimeout, "open connection", Transaction.Current.ToIdentifierString(), null,
                        "opening database connection to {ConnectionStringName} ({Provider})", connectionString.Name, connectionString.GetFriendlyProviderName());
                },
                onOpened: (connectionString, connection, retryCount) =>
                {
                    process.Context.RegisterIoCommandSuccess(process, IoCommandKind.dbConnection, iocUid, null);
                },
                onError: (connectionString, connection, retryCount, ex) =>
                {
                    process.Context.RegisterIoCommandFailed(process, IoCommandKind.dbConnection, iocUid, null, ex);

                    if (retryCount < maxRetryCount)
                    {
                        process.Context.Log(LogSeverity.Error, process, "can't connect to database, connection string key: {ConnectionStringName} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                            connectionString.Name, connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retryCount + 1), retryCount, ex.Message);

                        process.Context.LogOps(LogSeverity.Error, process, "can't connect to database, connection string key: {ConnectionStringName} ({Provider}), retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                            connectionString.Name, connectionString.GetFriendlyProviderName(), retryDelayMilliseconds * (retryCount + 1), retryCount, ex.Message);
                    }
                    else
                    {
                        var exception = new EtlException(process, "can't connect to database", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "can't connect to database, connection string key: {0}, message: {1}", connectionString.Name, ex.Message));
                        exception.Data.Add("ConnectionStringName", connectionString.Name);
                        exception.Data.Add("ProviderName", connectionString.ProviderName);
                        exception.Data.Add("NumberOfAttempts", maxRetryCount + 1);
                        throw exception;
                    }
                });

            return connection;
        }

        public static void ReleaseConnection(IProcess process, ref DatabaseConnection connection)
        {
            var iocUid = 0;

            _connectionManager.ReleaseConnection(connection,
            onClosing: connection =>
            {
                iocUid = process.Context.RegisterIoCommandStart(process, IoCommandKind.dbConnection, connection.ConnectionString.Name, null, "close connection", connection.TransactionWhenCreated.ToIdentifierString(), null,
                    "closing database connection to {ConnectionStringName} ({Provider})",
                        connection.ConnectionString.Name, connection.ConnectionString.GetFriendlyProviderName());
            },
            onClosed: connection =>
            {
                process.Context.RegisterIoCommandSuccess(process, IoCommandKind.dbConnection, iocUid, null);
            },
            onError: (connection, ex) =>
            {
                process.Context.RegisterIoCommandFailed(process, IoCommandKind.dbConnection, iocUid, null, ex);
            });

            if (connection == null)
                return;

            connection = null;
        }

        public static void ConnectionFailed(ref DatabaseConnection connection)
        {
            _connectionManager.ConnectionFailed(connection);
            connection = null;
        }

        public static void TestConnection(NamedConnectionString connectionString)
        {
            _connectionManager.TestConnection(connectionString);
        }
    }
}