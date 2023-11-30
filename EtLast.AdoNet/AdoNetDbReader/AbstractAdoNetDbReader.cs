namespace FizzCode.EtLast;

public delegate void ConnectionCreatorDelegate(AbstractAdoNetDbReader process, out DatabaseConnection connection, out IDbTransaction transaction);

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractAdoNetDbReader : AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required NamedConnectionString ConnectionString { get; init; }

    public Dictionary<string, ReaderColumn> Columns { get; init; }
    public ReaderColumn DefaultColumn { get; init; }

    /// <summary>
    /// If true, this process will execute out of ambient transaction scope. Default value is false.
    /// See <see cref="TransactionScopeOption.Suppress"/>.
    /// </summary>
    public bool SuppressExistingTransactionScope { get; init; }

    public ConnectionCreatorDelegate CustomConnectionCreator { get; init; }

    /// <summary>
    /// Default value is 3600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    public DateTime LastDataRead { get; private set; }
    public List<ISqlValueProcessor> SqlValueProcessors { get; } = [];

    public Dictionary<string, object> Parameters { get; init; }

    /// <summary>
    /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
    /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
    /// Default value is true.
    /// </summary>
    public bool InlineArrayParameters { get; init; } = true;

    protected abstract CommandType GetCommandType();

    protected AbstractAdoNetDbReader()
    {
        SqlValueProcessors.Add(new MySqlValueProcessor());
    }

    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> Produce()
    {
        var usedSqlValueProcessors = SqlValueProcessors.Where(x => x.Init(ConnectionString)).ToList();
        if (usedSqlValueProcessors.Count == 0)
            usedSqlValueProcessors = null;

        var sqlStatement = CreateSqlStatement();

        DatabaseConnection connection = null;
        IDbTransaction transaction = null;
        IDataReader reader = null;
        IDbCommand cmd = null;
        Stopwatch swQuery;

        var sqlStatementProcessed = InlineArrayParametersIfNecessary(sqlStatement);
        IoCommand ioCommand;

        using (var scope = Context.BeginTransactionScope(this, SuppressExistingTransactionScope ? TransactionScopeKind.Suppress : TransactionScopeKind.None, LogSeverity.Debug))
        {
            if (CustomConnectionCreator != null)
            {
                CustomConnectionCreator.Invoke(this, out connection, out transaction);
            }
            else
            {
                connection = EtlConnectionManager.GetConnection(ConnectionString, this);
            }

            cmd = connection.Connection.CreateCommand();
            cmd.CommandTimeout = CommandTimeout;
            cmd.CommandText = sqlStatementProcessed;
            cmd.CommandType = GetCommandType();
            cmd.Transaction = transaction;
            cmd.FillCommandParameters(Parameters);

            var transactionId = (CustomConnectionCreator != null && cmd.Transaction != null)
                ? "custom (" + cmd.Transaction.IsolationLevel.ToString() + ")"
                : Transaction.Current.ToIdentifierString();

            ioCommand = RegisterIoCommand(transactionId, cmd.CommandTimeout, sqlStatement);

            swQuery = Stopwatch.StartNew();
            try
            {
                reader = cmd.ExecuteReader();
            }
            catch (Exception ex)
            {
                var exception = new SqlReadException(this, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while executing query, message: {0}, connection string key: {1}, SQL statement: {2}",
                    ex.Message, ConnectionString.Name, sqlStatement));
                exception.Data["ConnectionStringName"] = ConnectionString.Name;
                exception.Data["Statement"] = cmd.CommandText;

                ioCommand.Exception = exception;
                Context.RegisterIoCommandEnd(this, ioCommand);
                throw exception;
            }
        }

        LastDataRead = DateTime.Now;

        var resultCount = 0L;
        if (reader != null && !FlowState.IsTerminating)
        {
            var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            // key is the SOURCE column name
            var columnMap = Columns?.ToDictionary(kvp => kvp.Value?.SourceColumn ?? kvp.Key, kvp => (rowColumn: kvp.Key, config: kvp.Value), StringComparer.InvariantCultureIgnoreCase);

            var fieldCount = reader.FieldCount;
            var columns = new MappedColumn[fieldCount];
            for (var i = 0; i < fieldCount; i++)
            {
                var fieldName = reader.GetName(i);

                if (DefaultColumn != null)
                {
                    columns[i] = columnMap != null && columnMap.TryGetValue(fieldName, out var cc)
                        ? new MappedColumn()
                        {
                            NameInRow = cc.rowColumn,
                            Config = cc.config ?? DefaultColumn,
                        }
                        : new MappedColumn()
                        {
                            NameInRow = fieldName,
                            Config = DefaultColumn,
                        };
                }
                else if (columnMap != null)
                {
                    if (columnMap.TryGetValue(fieldName, out var cc))
                    {
                        columns[i] = new MappedColumn()
                        {
                            NameInRow = fieldName,
                            Config = cc.config,
                        };
                    }
                }
                else
                {
                    columns[i] = new MappedColumn()
                    {
                        NameInRow = fieldName,
                        Config = null,
                    };
                }
            }

            while (!FlowState.IsTerminating)
            {
                try
                {
                    if (!reader.Read())
                        break;
                }
                catch (Exception ex)
                {
                    var exception = new SqlReadException(this, ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while executing query after successfully reading {0} rows, message: {1}, connection string key: {2}, SQL statement: {3}",
                        resultCount, ex.Message, ConnectionString.Name, sqlStatement));
                    exception.Data["ConnectionStringName"] = ConnectionString.Name;
                    exception.Data["Statement"] = cmd.CommandText;
                    exception.Data["RowIndex"] = resultCount;
                    exception.Data["SecondsSinceLastRead"] = LastDataRead.Subtract(DateTime.Now).TotalSeconds.ToString(CultureInfo.InvariantCulture);

                    ioCommand.Exception = exception;
                    ioCommand.AffectedDataCount += resultCount;
                    Context.RegisterIoCommandEnd(this, ioCommand);
                    throw exception;
                }

                LastDataRead = DateTime.Now;

                initialValues.Clear();
                for (var i = 0; i < fieldCount; i++)
                {
                    var column = columns[i];
                    if (column == null)
                        continue;

                    var value = reader.GetValue(i);
                    if (value is DBNull)
                        value = null;

                    if (usedSqlValueProcessors != null)
                    {
                        foreach (var processor in usedSqlValueProcessors)
                        {
                            value = processor.ProcessValue(value);
                        }
                    }

                    if (column.Config != null)
                    {
                        try
                        {
                            value = column.Config.Process(this, value);
                        }
                        catch (Exception ex)
                        {
                            value = new EtlRowError(this, value, ex);
                        }
                    }

                    initialValues[column.NameInRow] = value;
                }

                resultCount++;
                yield return Context.CreateRow(this, initialValues);
            }
        }

        ioCommand.AffectedDataCount += resultCount;
        Context.RegisterIoCommandEnd(this, ioCommand);

        if (reader != null)
        {
            try
            {
                reader.Close();
                reader.Dispose();
                reader = null;
            }
            catch (Exception)
            {
                reader = null;
            }
        }

        if (cmd != null)
        {
            try
            {
                cmd.Dispose();
                cmd = null;
            }
            catch (Exception)
            {
                cmd = null;
            }
        }

        if (CustomConnectionCreator == null)
        {
            EtlConnectionManager.ReleaseConnection(this, ref connection);
        }
    }

    private string InlineArrayParametersIfNecessary(string sqlStatement)
    {
        if (InlineArrayParameters && Parameters != null)
        {
            var parameters = Parameters.ToList();
            foreach (var kvp in parameters)
            {
                var paramReference = "@" + kvp.Key;

                var startIndex = 0;
                while (startIndex < sqlStatement.Length - paramReference.Length) // handle multiple occurrences
                {
                    var idx = sqlStatement.IndexOf(paramReference, startIndex, StringComparison.InvariantCultureIgnoreCase);
                    if (idx == -1)
                        break;

                    string newParamText = null;

                    if (kvp.Value is int[] intArray)
                    {
                        newParamText = string.Join(",", intArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    }
                    else if (kvp.Value is long[] longArray)
                    {
                        newParamText = string.Join(",", longArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    }
                    else if (kvp.Value is string[] stringArray)
                    {
                        var sb = new StringBuilder();
                        foreach (var s in stringArray)
                        {
                            if (sb.Length > 0)
                                sb.Append(',');

                            sb.Append('\'');
                            sb.Append(s);
                            sb.Append('\'');
                        }

                        newParamText = sb.ToString();
                    }
                    else if (kvp.Value is List<int> intList)
                    {
                        newParamText = string.Join(",", intList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    }
                    else if (kvp.Value is List<long> longList)
                    {
                        newParamText = string.Join(",", longList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    }
                    else if (kvp.Value is List<string> stringList)
                    {
                        var sb = new StringBuilder();
                        foreach (var s in stringList)
                        {
                            if (sb.Length > 0)
                                sb.Append(',');

                            sb.Append('\'');
                            sb.Append(s);
                            sb.Append('\'');
                        }

                        newParamText = sb.ToString();
                    }

                    if (newParamText != null)
                    {
                        sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));
                        startIndex = idx + newParamText.Length;

                        Parameters.Remove(kvp.Key);
                    }
                    else
                    {
                        startIndex += paramReference.Length;
                    }
                }
            }
        }

        return sqlStatement;
    }

    private class MappedColumn
    {
        public string NameInRow { get; init; }
        public ReaderColumn Config { get; init; }
    }

    protected abstract IoCommand RegisterIoCommand(string transactionId, int timeout, string statement);
    protected abstract string CreateSqlStatement();
}
