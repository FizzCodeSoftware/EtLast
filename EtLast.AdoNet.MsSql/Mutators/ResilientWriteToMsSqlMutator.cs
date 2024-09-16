namespace FizzCode.EtLast;

public sealed class ResilientWriteToMsSqlMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue]
    public MsSqlConnectionString ConnectionString { get; init; }

    /// <summary>
    /// Default value is 3600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    [ProcessParameterMustHaveValue]
    public required string TableName { get; init; }

    /// <summary>
    /// Key is column in the row, value is column in the database table (can be null).
    /// </summary>
    [ProcessParameterMustHaveValue]
    public Dictionary<string, string> Columns { get; init; }

    /// <summary>
    /// Default value is true <see cref="SqlBulkCopyOptions.KeepIdentity"/>.
    /// </summary>
    public bool BulkCopyKeepIdentity { get; init; } = true;

    /// <summary>
    /// Default value is false <see cref="SqlBulkCopyOptions.CheckConstraints"/>.
    /// </summary>
    public bool BulkCopyCheckConstraints { get; init; }

    /// <summary>
    /// Default value is 10000
    /// </summary>
    public int BatchSize { get; init; } = 10000;

    /// <summary>
    /// Default value is 5
    /// </summary>
    public int MaxRetryCount { get; init; } = 5;

    /// <summary>
    /// Default value is 5000
    /// </summary>
    public int RetryDelayMilliseconds { get; init; } = 5000;

    /// <summary>
    /// Default value is 5000
    /// </summary>
    public int ForcedFlushInterval { get; init; } = 5000;

    private long _rowsWritten;
    private Stopwatch _timer;
    private RowShadowReader _reader;
    private Sink _sink;
    private Stopwatch _lastWrite;

    protected override void StartMutator()
    {
        _rowsWritten = 0;
        _timer = new Stopwatch();
        _lastWrite = null;

        var columnIndexes = new Dictionary<string, int>();
        var i = 0;
        foreach (var column in Columns)
        {
            columnIndexes[column.Key] = i;
            i++;
        }

        _reader = new RowShadowReader(BatchSize, Columns.Select(column => column.Value ?? column.Key).ToArray(), columnIndexes);
    }

    protected override void CloseMutator()
    {
        if (_reader.RowCount > 0)
        {
            WriteToSql();
        }

        _reader = null;

        _timer.Stop();
        _timer = null;
    }

    protected override void ProcessHeartBeatTag(HeartBeatTag tag)
    {
        if (_rowsWritten > 0 && _reader.RowCount > 0 && _reader.RowCount < BatchSize && (_lastWrite == null || _lastWrite.ElapsedMilliseconds >= ForcedFlushInterval))
        {
            WriteToSql();
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        _sink ??= Context.GetSink(ConnectionString.Name, ConnectionString.Unescape(TableName), "sql", this,
            Columns.Select(x => x.Value ?? x.Key).ToArray());

        _sink.RegisterRow(row);

        var rc = _reader.RowCount;
        var i = 0;
        foreach (var column in Columns)
        {
            _reader.Rows[rc, i] = row[column.Key];
            i++;
        }

        rc++;
        _reader.RowCount = rc;

        if (rc >= BatchSize)
        {
            WriteToSql();
        }

        yield return row;
    }

    private void WriteToSql()
    {
        var recordCount = _reader.RowCount;
        _timer.Restart();

        if (_lastWrite == null)
        {
            _lastWrite = new Stopwatch();
        }
        else
        {
            _lastWrite.Restart();
        }

        for (var retry = 0; retry <= MaxRetryCount; retry++)
        {
            DatabaseConnection connection = null;
            SqlBulkCopy bulkCopy = null;

            try
            {
                using (var scope = new EtlTransactionScope(this, TransactionScopeKind.RequiresNew, LogSeverity.Debug))
                {
                    var transactionId = Transaction.Current.ToIdentifierString();

                    connection = EtlConnectionManager.GetConnection(ConnectionString, this, 0);

                    var options = SqlBulkCopyOptions.Default;

                    if (BulkCopyKeepIdentity)
                        options |= SqlBulkCopyOptions.KeepIdentity;

                    if (BulkCopyCheckConstraints)
                        options |= SqlBulkCopyOptions.CheckConstraints;

                    bulkCopy = new SqlBulkCopy(connection.Connection as SqlConnection, options, null)
                    {
                        DestinationTableName = TableName,
                        BulkCopyTimeout = CommandTimeout,
                    };

                    foreach (var column in Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.Key, column.Value ?? column.Key);
                    }

                    var ioCommand = Context.RegisterIoCommand(new IoCommand()
                    {
                        Process = this,
                        Kind = IoCommandKind.dbWriteBulk,
                        Location = ConnectionString.Name,
                        Path = ConnectionString.Unescape(TableName),
                        TimeoutSeconds = bulkCopy.BulkCopyTimeout,
                        Command = "BULK COPY into " + TableName + ", " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records" + (retry > 0 ? ", retry #" + retry.ToString("D", CultureInfo.InvariantCulture) : ""),
                        TransactionId = transactionId,
                        Message = "write to table",
                    });

                    var success = false;
                    try
                    {
                        bulkCopy.WriteToServer(_reader);
                        bulkCopy.Close();
                        EtlConnectionManager.ReleaseConnection(this, ref connection);

                        ioCommand.AffectedDataCount += recordCount;
                        ioCommand.End();
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        var exception = new SqlWriteException(this, ex);
                        exception.Data["ConnectionStringName"] = ConnectionString.Name;
                        exception.Data["TableName"] = ConnectionString.Unescape(TableName);
                        exception.Data["Columns"] = string.Join(", ", Columns.Select(column => column.Key + " => " + ConnectionString.Unescape(column.Value ?? column.Key)));
                        exception.Data["Timeout"] = CommandTimeout;
                        exception.Data["Elapsed"] = _timer.Elapsed;
                        exception.Data["TotalRowsWritten"] = _rowsWritten;

                        ioCommand.AffectedDataCount += recordCount;
                        ioCommand.Failed(exception);

                        throw; // by design. do not "throw exception"
                    }

                    if (success)
                    {
                        scope.Complete();
                        _rowsWritten += recordCount;
                        _reader.Reset();
                        break;
                    }
                } // dispose scope
            }
            catch (Exception ex) // catch previously thrown SqlWriteException during write OR an exception thrown by scope.Complete()
            {
                if (connection != null)
                {
                    EtlConnectionManager.ConnectionFailed(ref connection);
                    bulkCopy?.Close();
                }

                _reader.ResetCurrentIndex();

                if (retry == 0 && (ex is InvalidOperationException || ex is SqlException))
                {
                    var fileName = "bulk-copy-error-" + Context.Manifest.CreatedOnLocal.ToString("yyyy-MM-dd-HH-mm-ss", CultureInfo.InvariantCulture) + "-" + ExecutionInfo.Id.ToString("D", CultureInfo.InvariantCulture) + ".tsv";
                    Context.LogCustom(fileName, this, "bulk copy error: " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableName) + ", exception: " + ex.GetType().GetFriendlyTypeName() + ": " + ex.Message);
                    Context.LogCustom(fileName, this, string.Join("\t", _reader.ColumnIndexes.Select(kvp => kvp.Key)));

                    for (var row = 0; row < _reader.RowCount; row++)
                    {
                        var text = string.Join("\t", _reader.ColumnIndexes.Select(kvp =>
                        {
                            var v = _reader.Rows[row, kvp.Value];
                            return v == null
                                ? "NULL"
                                : "'" + v.ToString() + "' (" + v.GetType().GetFriendlyTypeName() + ")";
                        }));

                        Context.LogCustom(fileName, this, text);
                    }
                }

                if (retry < MaxRetryCount)
                {
                    Context.Log(LogSeverity.Error, this, "db write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", RetryDelayMilliseconds * (retry + 1),
                        retry, ex.Message);

                    Context.LogOps(LogSeverity.Error, this, "db write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", Name,
                        RetryDelayMilliseconds * (retry + 1), retry, ex.Message);

                    Thread.Sleep(RetryDelayMilliseconds * (retry + 1));
                }
                else
                {
                    var exception = new SqlWriteException(this, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}",
                        ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message));
                    exception.Data["ConnectionStringName"] = ConnectionString.Name;
                    exception.Data["TableName"] = ConnectionString.Unescape(TableName);
                    exception.Data["Columns"] = string.Join(", ", Columns.Select(column => column.Key + " => " + ConnectionString.Unescape(column.Value ?? column.Key)));
                    exception.Data["Timeout"] = CommandTimeout;
                    exception.Data["Elapsed"] = _timer.Elapsed;
                    exception.Data["TotalRowsWritten"] = _rowsWritten;
                    throw exception;
                }
            }
        }
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.SqlEngine != AdoNetEngine.MsSql)
            throw new InvalidProcessParameterException(this, "ConnectionString", nameof(ConnectionString.ProviderName), "provider name must be Microsoft.Data.SqlClient");
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MsSqlWriteWithMicroTransactionsMutatorFluent
{
    /// <summary>
    /// Write rows to a Microsoft SQL database table in batches, using <see cref="SqlBulkCopy"/>.
    /// <para>Creates a new transaction scope for each batch which suppress any existing transaction scope.</para>
    /// <para>Retrying the SQL operation is supported and enabled by default.</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToMsSqlResilient(this IFluentSequenceMutatorBuilder builder, ResilientWriteToMsSqlMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
