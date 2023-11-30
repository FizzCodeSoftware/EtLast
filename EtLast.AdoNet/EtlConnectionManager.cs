namespace FizzCode.EtLast;

public static class EtlConnectionManager
{
    private static readonly ConnectionManager _connectionManager = new()
    {
        SeparateConnectionsByThreadId = false,
    };

    public static DatabaseConnection GetConnection(NamedConnectionString connectionString, IProcess process, int maxRetryCount = 5, int retryDelayMilliseconds = 2000)
    {
        if (string.IsNullOrEmpty(connectionString.ProviderName))
        {
            var ex = new EtlException(process, "missing provider name for connection string");
            ex.Data["ConnectionStringName"] = connectionString.Name;
            ex.AddOpsMessage("missing provider name for connection string key: " + connectionString.Name);
            throw ex;
        }

        IoCommand ioCommand = null;

        return _connectionManager.GetConnection(connectionString, maxRetryCount, retryDelayMilliseconds,
            onOpening: (connectionString, connection) =>
            {
                ioCommand = process.Context.RegisterIoCommandStart(new IoCommand()
                {
                    Process = process,
                    Kind = IoCommandKind.dbConnection,
                    Location = connectionString.Name,
                    TimeoutSeconds = connection.ConnectionTimeout,
                    TransactionId = Transaction.Current.ToIdentifierString(),
                    Message = "opening database connection",
                    MessageExtra = connectionString.GetFriendlyProviderName(),
                });
            },
            onOpened: (connectionString, connection, retryCount) => ioCommand.End(),
            onError: (connectionString, connection, retryCount, ex) =>
            {
                ioCommand.Failed(ex);

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
                    exception.Data["ConnectionStringName"] = connectionString.Name;
                    exception.Data["ProviderName"] = connectionString.ProviderName;
                    exception.Data["NumberOfAttempts"] = maxRetryCount + 1;
                    throw exception;
                }
            });
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

        IoCommand ioCommand = null;

        var connection = _connectionManager.GetNewConnection(connectionString, maxRetryCount, retryDelayMilliseconds,
            onOpening: (connectionString, connection) =>
            {
                ioCommand = process.Context.RegisterIoCommandStart(new IoCommand()
                {
                    Process = process,
                    Kind = IoCommandKind.dbConnection,
                    Location = connectionString.Name,
                    TimeoutSeconds = connection.ConnectionTimeout,
                    TransactionId = Transaction.Current.ToIdentifierString(),
                    Message = "opening database connection",
                    MessageExtra = connectionString.GetFriendlyProviderName(),
                });
            },
            onOpened: (connectionString, connection, retryCount) => ioCommand.End(),
            onError: (connectionString, connection, retryCount, ex) =>
            {
                ioCommand.Failed(ex);

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
                    exception.Data["ConnectionStringName"] = connectionString.Name;
                    exception.Data["ProviderName"] = connectionString.ProviderName;
                    exception.Data["NumberOfAttempts"] = maxRetryCount + 1;
                    throw exception;
                }
            });

        return connection;
    }

    public static void ReleaseConnection(IProcess process, ref DatabaseConnection connection)
    {
        IoCommand ioCommand = null;

        _connectionManager.ReleaseConnection(connection,
        onClosing: connection =>
        {
            ioCommand = process.Context.RegisterIoCommandStart(new IoCommand()
            {
                Process = process,
                Kind = IoCommandKind.dbConnection,
                Location = connection.ConnectionString.Name,
                TransactionId = connection.TransactionWhenCreated.ToIdentifierString(),
                Message = "closing database connection",
                MessageExtra = connection.ConnectionString.GetFriendlyProviderName(),
            });
        },
        onClosed: connection => ioCommand.End(),
        onError: (connection, ex) =>
        {
            ioCommand.Failed(ex);
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
