namespace FizzCode.EtLast;

public sealed class WriteToMsSqlMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

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
    /// Default value is 5000
    /// </summary>
    public int ForcedFlushInterval { get; init; } = 5000;

    private long _rowsWritten;
    private DatabaseConnection _connection;
    private SqlBulkCopy _bulkCopy;
    private RowShadowReader _reader;
    private long? _sinkUid;
    private Stopwatch _lastWrite;

    protected override void StartMutator()
    {
        _rowsWritten = 0;

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
            InitConnection();
            lock (_connection.Lock)
            {
                WriteToSql();
            }
        }

        _reader = null;

        if (_bulkCopy != null)
        {
            _bulkCopy.Close();
            _bulkCopy = null;
        }

        EtlConnectionManager.ReleaseConnection(this, ref _connection);
    }

    protected override void ProcessHeartBeatTag(HeartBeatTag tag)
    {
        if (_rowsWritten > 0 && _reader.RowCount > 0 && _reader.RowCount < BatchSize && (_lastWrite == null || _lastWrite.ElapsedMilliseconds >= ForcedFlushInterval))
        {
            InitConnection();
            lock (_connection.Lock)
            {
                WriteToSql();
            }
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        _sinkUid ??= Context.GetSinkUid(ConnectionString.Name, ConnectionString.Unescape(TableName));

        Context.RegisterWriteToSink(row, _sinkUid.Value);

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
            InitConnection();
            lock (_connection.Lock)
            {
                WriteToSql();
            }
        }

        yield return row;
    }

    private void WriteToSql()
    {
        if (_lastWrite == null)
        {
            _lastWrite = new Stopwatch();
        }
        else
        {
            _lastWrite.Restart();
        }

        if (Transaction.Current == null)
            Context.Log(LogSeverity.Warning, this, "there is no active transaction!");

        var recordCount = _reader.RowCount;

        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbWriteBulk, ConnectionString.Name, ConnectionString.Unescape(TableName), _bulkCopy.BulkCopyTimeout, "BULK COPY " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records", Transaction.Current.ToIdentifierString(), null,
            "write to table", null);

        try
        {
            _bulkCopy.WriteToServer(_reader);

            _rowsWritten += recordCount;

            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteBulk, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            EtlConnectionManager.ReleaseConnection(this, ref _connection);
            _bulkCopy.Close();
            _bulkCopy = null;

            var exception = new SqlWriteException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message));
            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Columns"] = string.Join(", ", Columns.Select(column => column.Key + " => " + ConnectionString.Unescape(column.Value ?? column.Key)));
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["TotalRowsWritten"] = _rowsWritten;
            if (ex is InvalidOperationException or SqlException)
            {
                var fileName = "bulk-copy-error-" + Context.CreatedOnLocal.ToString("yyyy-MM-dd HH-mm-ss", CultureInfo.InvariantCulture) + ".tsv";
                exception.Data["DetailedRowLogFileName"] = fileName;
                Context.LogCustom(fileName, this, "bulk copy error: " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableName) + "] = exception: " + ex.GetType().GetFriendlyTypeName() + ": " + ex.Message);
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

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteBulk, iocUid, recordCount, exception);
            throw exception;
        }

        _reader.Reset();
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.SqlEngine != SqlEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    private void InitConnection()
    {
        if (_connection != null)
            return;

        _connection = EtlConnectionManager.GetConnection(ConnectionString, this);

        var options = SqlBulkCopyOptions.Default;

        if (BulkCopyKeepIdentity)
            options |= SqlBulkCopyOptions.KeepIdentity;

        if (BulkCopyCheckConstraints)
            options |= SqlBulkCopyOptions.CheckConstraints;

        _bulkCopy = new SqlBulkCopy(_connection.Connection as SqlConnection, options, null)
        {
            DestinationTableName = TableName,
            BulkCopyTimeout = CommandTimeout,
        };

        foreach (var kvp in Columns)
        {
            _bulkCopy.ColumnMappings.Add(kvp.Key, kvp.Value ?? kvp.Key);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class MsSqlWriteMutatorFluent
{
    /// <summary>
    /// Write rows to a Microsoft SQL database table in batches, using <see cref="SqlBulkCopy"/>.
    /// <para>Does not create or suppress any transaction scope.</para>
    /// <para>Does not support retrying the SQL operation and any failure will put the ETL context into a failed state.</para>
    /// <para>It is not recommended to use this mutator to access a remote SQL database.</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder WriteToMsSql(this IFluentSequenceMutatorBuilder builder, WriteToMsSqlMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}