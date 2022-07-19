namespace FizzCode.EtLast;

public sealed class ResilientWriteToMsSqlMutator : AbstractMutator, IRowSink
{
    public NamedConnectionString ConnectionString { get; init; }

    /// <summary>
    /// Default value is 3600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    public DbTableDefinition TableDefinition { get; init; }

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
    public int ForceWriteAfterNoDataMilliseconds { get; init; } = 5000;

    private int _rowsWritten;
    private Stopwatch _timer;
    private RowShadowReader _reader;
    private int? _sinkUid;
    private Stopwatch _lastWrite;

    public ResilientWriteToMsSqlMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _rowsWritten = 0;
        _timer = new Stopwatch();
        _lastWrite = null;

        var columnIndexes = new Dictionary<string, int>();
        var i = 0;
        foreach (var column in TableDefinition.Columns)
        {
            columnIndexes[column.Key] = i;
            i++;
        }

        _reader = new RowShadowReader(BatchSize, TableDefinition.Columns.Select(column => column.Value ?? column.Key).ToArray(), columnIndexes);
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

    protected override void ProcessHeartBeatRow(IReadOnlySlimRow row, HeartBeatTag tag)
    {
        if (_rowsWritten > 0 && _lastWrite != null && _reader.RowCount > 0 && _reader.RowCount < BatchSize && _lastWrite.ElapsedMilliseconds >= ForceWriteAfterNoDataMilliseconds)
        {
            WriteToSql();
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (_sinkUid == null)
            _sinkUid = Context.GetSinkUid(ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));

        Context.RegisterWriteToSink(row, _sinkUid.Value);

        var rc = _reader.RowCount;
        var i = 0;
        foreach (var column in TableDefinition.Columns)
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
                using (var scope = Context.BeginScope(this, TransactionScopeKind.RequiresNew, LogSeverity.Debug))
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
                        DestinationTableName = TableDefinition.TableName,
                        BulkCopyTimeout = CommandTimeout,
                    };

                    foreach (var column in TableDefinition.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.Key, column.Value ?? column.Key);
                    }

                    var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteBulk, ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), bulkCopy.BulkCopyTimeout, "BULK COPY into " + TableDefinition.TableName + ", " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records" + (retry > 0 ? ", retry #" + retry.ToString("D", CultureInfo.InvariantCulture) : ""), Transaction.Current.ToIdentifierString(), null,
                        "write to table: {ConnectionStringName}/{Table}",
                        ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));

                    var success = false;
                    try
                    {
                        bulkCopy.WriteToServer(_reader);
                        bulkCopy.Close();
                        EtlConnectionManager.ReleaseConnection(this, ref connection);

                        Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteBulk, iocUid, recordCount);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteBulk, iocUid, recordCount, ex);
                    }

                    if (success)
                        scope.Complete();
                } // dispose scope

                _rowsWritten += recordCount;
                _reader.Reset();
                break;
            }
            catch (Exception ex)
            {
                if (connection != null)
                {
                    EtlConnectionManager.ConnectionFailed(ref connection);
                    bulkCopy?.Close();
                }

                _reader.ResetCurrentIndex();

                if (retry == 0 && (ex is InvalidOperationException || ex is SqlException))
                {
                    var fileName = "bulk-copy-error-" + Context.CreatedOnLocal.ToString("yyyy-MM-dd HH-mm-ss", CultureInfo.InvariantCulture) + ".tsv";
                    Context.LogCustom(fileName, this, "bulk copy error: " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName) + ", exception: " + ex.GetType().GetFriendlyTypeName() + ": " + ex.Message);
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
                        ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message));
                    exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                    exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                    exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(column => column.Key + " => " + ConnectionString.Unescape(column.Value ?? column.Key))));
                    exception.Data.Add("Timeout", CommandTimeout);
                    exception.Data.Add("Elapsed", _timer.Elapsed);
                    exception.Data.Add("TotalRowsWritten", _rowsWritten);
                    if (ex is InvalidOperationException or SqlException)
                    {
                        var fileName = "bulk-copy-error-" + Context.CreatedOnLocal.ToString("yyyy-MM-dd HH-mm-ss", CultureInfo.InvariantCulture) + ".tsv";
                        exception.Data.Add("DetailedRowLogFileName", fileName);
                    }

                    throw exception;
                }
            }
        }
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));

        if (TableDefinition == null)
            throw new ProcessParameterNullException(this, nameof(TableDefinition));

        if (TableDefinition.Columns == null)
            throw new ProcessParameterNullException(this, nameof(TableDefinition) + "." + nameof(TableDefinition.Columns));

        if (ConnectionString.SqlEngine != SqlEngine.MsSql)
            throw new InvalidProcessParameterException(this, "ConnectionString", nameof(ConnectionString.ProviderName), "provider name must be Microsoft.Data.SqlClient");
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
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
