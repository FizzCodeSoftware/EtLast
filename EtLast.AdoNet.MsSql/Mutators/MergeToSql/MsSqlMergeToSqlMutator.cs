namespace FizzCode.EtLast;

public sealed class MsSqlMergeToSqlMutator : AbstractMutator, IRowSink
{
    [ProcessParameterMustHaveValue]
    public MsSqlConnectionString ConnectionString { get; init; }

    /// <summary>
    /// Default value is 3600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    /// <summary>
    /// Default value is 30.
    /// </summary>
    public int MaximumParameterCount { get; init; } = 30;

    [ProcessParameterMustHaveValue]
    public required string TableName { get; init; }

    [ProcessParameterMustHaveValue]
    public required DbColumn[] KeyColumns { get; set; }

    /// <summary>
    /// If set to null, the column names will be used (and escaped) from the first row's <see cref="IReadOnlySlimRow.Values"/>, except the columns set in <see cref="KeyColumns"/>.
    /// </summary>
    public required DbColumn[] ValueColumns { get; set; }

    /// <summary>
    /// Default value is 5000
    /// </summary>
    public int ForceWriteAfterNoDataMilliseconds { get; init; } = 5000;

    private DatabaseConnection _connection;
    private List<string> _statements;

    private long _rowsWritten;

    private IDbCommand _command;
    private Sink _sink;
    private Stopwatch _lastWrite;
    private bool _prepared = false;

    private string _tableName;
    private DbColumn[] _allColumns;
    private string _allDbColumns;
    private string _keyDbColumns;
    private string _updateDbColumns;
    private string _insertDbColumnsTarget;
    private string _insertDbColumnsSource;

    protected override void StartMutator()
    {
        _prepared = false;
        _rowsWritten = 0;
        _statements = [];
    }

    private void Prepare()
    {
        if (_prepared)
            return;

        _prepared = true;
        _tableName = TableName;
        _allColumns = [.. KeyColumns, .. ValueColumns];

        _allDbColumns = string.Join(", ", ValueColumns.Select(x => x.NameInDatabase));
        _keyDbColumns = string.Join(" AND ", KeyColumns.Select(x => "target." + x.NameInDatabase + " = source." + x.NameInDatabase));
        _updateDbColumns = string.Join(",\n\t\t", ValueColumns.Select(x => x.NameInDatabase + " = source." + x.NameInDatabase));
        _insertDbColumnsTarget = string.Join(", ", _allColumns.Where(x => x.Insert).Select(x => x.NameInDatabase));
        _insertDbColumnsSource = string.Join(", ", _allColumns.Where(x => x.Insert).Select(x => "source." + x.NameInDatabase));
    }

    protected override void CloseMutator()
    {
        if (_command != null)
        {
            ExecuteStatements();
        }

        _statements = null;

        EtlConnectionManager.ReleaseConnection(this, ref _connection);
    }

    protected override void ProcessHeartBeatTag(HeartBeatTag tag)
    {
        if (_rowsWritten > 0 && _lastWrite != null && _command != null && _statements.Count > 0 && _lastWrite.ElapsedMilliseconds >= ForceWriteAfterNoDataMilliseconds)
        {
            lock (_connection.Lock)
            {
                ExecuteStatements();
            }
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        _sink ??= Context.GetSink(ConnectionString.Name, ConnectionString.Unescape(TableName), "sql", this,
            KeyColumns.Concat(ValueColumns).Distinct().Select(x => x.NameInDatabase).ToArray());

        _sink.RegisterWrite(row);

        InitConnection();

        ValueColumns ??= row.Values
            .Where(x => !KeyColumns.Any(kc => kc.RowColumn == x.Key))
            .Select(x => new DbColumn(x.Key, ConnectionString.Escape(x.Key)))
            .ToArray();

        Prepare();

        lock (_connection.Lock)
        {
            if (_command == null)
            {
                _command = _connection.Connection.CreateCommand();
                _command.CommandTimeout = CommandTimeout;
            }

            var startIndex = ParameterCount;
            foreach (var column in _allColumns)
                CreateParameter(column.DbType, row[column.RowColumn]);

            var statement = "(" + string.Join(", ", _allColumns.Select(_ => "@" + startIndex++.ToString("D", CultureInfo.InvariantCulture))) + ")";
            _statements.Add(statement);

            if (_command.Parameters.Count >= MaximumParameterCount - 1)
            {
                ExecuteStatements();
            }
        }

        yield return row;
    }

    private void InitConnection()
    {
        if (_connection != null)
            return;

        try
        {
            _connection = EtlConnectionManager.GetConnection(ConnectionString, this);
        }
        catch (Exception ex)
        {
            var exception = new SqlConnectionException(this, ex);
            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            throw exception;
        }
    }

    public int ParameterCount => _command?.Parameters.Count ?? 0;

    [EditorBrowsable(EditorBrowsableState.Never)]
    public void CreateParameter(DbType? dbType, object value)
    {
        var parameter = _command.CreateParameter();
        parameter.ParameterName = "@" + _command.Parameters.Count.ToString("D", CultureInfo.InvariantCulture);

        parameter.SetValue(value, dbType);

        _command.Parameters.Add(parameter);
    }

    private void ExecuteStatements()
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

        var sqlStatement = "MERGE INTO " + _tableName + " target USING (VALUES \n"
            + string.Join(", ", _statements) + "\n) AS source (" + _allDbColumns + ")\nON " + _keyDbColumns
            + (!string.IsNullOrEmpty(_updateDbColumns) ? "\nWHEN MATCHED THEN\n\tUPDATE SET\n\t\t" + _updateDbColumns : "")
            + "\nWHEN NOT MATCHED BY TARGET THEN\n\tINSERT (" + _insertDbColumnsTarget + ")\n\tVALUES (" + _insertDbColumnsSource + ");";

        var recordCount = _statements.Count;

        _command.CommandText = sqlStatement;

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbWriteMerge,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = _command.CommandTimeout,
            Command = _command.CommandText,
            TransactionId = Transaction.Current.ToIdentifierString(),
            Message = "merge to table",
        });

        try
        {
            _command.ExecuteNonQuery();

            _rowsWritten += recordCount;

            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlWriteException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}, statement: {3}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, sqlStatement));
            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["KeyColumns"] = string.Join(", ", KeyColumns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.NameInDatabase)));
            exception.Data["ValueColumns"] = string.Join(", ", ValueColumns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.NameInDatabase)));
            exception.Data["SqlStatement"] = sqlStatement;
            exception.Data["SqlStatementCompiled"] = _command.CompileSql();
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["TotalRowsWritten"] = _rowsWritten;

            ioCommand.AffectedDataCount += recordCount;
            ioCommand.Failed(exception);
            throw exception;
        }

        _command = null;
        _statements.Clear();
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MergeToSqlMutatorFluent
{
    /// <summary>
    /// Merge rows into a database table in batches, using statements generated by any implementation of <see cref="IMergeToSqlStatementCreator"/>.
    /// <para>Doesn't create or suppress any transaction scope.</para>
    /// <para>Doesn't support retrying the SQL operation and any failure will put the flow into a failed state.</para>
    /// <para>It is not recommended to use this mutator to access a remote SQL database.</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder MergeToSql(this IFluentSequenceMutatorBuilder builder, MsSqlMergeToSqlMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}