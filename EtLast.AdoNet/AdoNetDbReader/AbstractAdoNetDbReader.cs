﻿namespace FizzCode.EtLast;

public delegate void ConnectionCreatorDelegate(AbstractAdoNetDbReader process, out DatabaseConnection connection, out IDbTransaction transaction);

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractAdoNetDbReader : AbstractRowSource
{
    [ProcessParameterMustHaveValue]
    public required IAdoNetSqlConnectionString ConnectionString { get; init; }

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

    /// <summary>
    /// If initialized with a list, then the column info returned by the ADO.NET connector based on the given query will be stored in it.
    /// </summary>
    public List<DataTypeInfo> ColumnInfoList { get; init; } = [];

    public Action<IReadOnlyList<DataTypeInfo>> ColumnInfoListTester { get; init; }

    public Func<string, string> SqlStatementCustomizer { get; init; }

    protected abstract CommandType GetCommandType();

    protected AbstractAdoNetDbReader()
    {
        SqlValueProcessors.Add(new MySqlValueProcessor());
        SqlValueProcessors.Add(new MsSqlValueProcessor());
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

        if (SqlStatementCustomizer != null)
            sqlStatement = SqlStatementCustomizer.Invoke(sqlStatement);

        IoCommand ioCommand;

        try
        {
            using (var scope = new EtlTransactionScope(this, SuppressExistingTransactionScope ? TransactionScopeKind.Suppress : TransactionScopeKind.None, LogSeverity.Debug))
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

                    ioCommand.Failed(exception);
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

                var schemaTable = ColumnInfoList != null
                    ? reader.GetSchemaTable()
                    : null;

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

                    if (schemaTable != null && columns[i] != null)
                    {
                        var schemaRow = schemaTable.Rows[i];
                        var properties = new Dictionary<string, object>();
                        foreach (DataColumn c in schemaTable.Columns)
                        {
                            var fieldValue = schemaRow[c];
                            if (fieldValue != null && fieldValue is not DBNull)
                            {
                                if (fieldValue is bool boolFieldValue && !boolFieldValue)
                                    continue;

                                if (fieldName is "ColumnName" or "ColumnOrdinal" or "ProviderType" or "NonVersionedProviderType"
                                    or "ProviderSpecificDataType" or "BaseColumnName" or "BaseTableName" or "BaseCatalogName" or "BaseSchemaName")
                                {
                                    continue;
                                }

                                properties[c.ColumnName] = fieldValue is Type type
                                    ? type.Name
                                    : fieldValue;
                            }
                        }

                        var dataTypeName = (string)null;
                        if (properties.TryGetValue("DataTypeName", out var v) && v is string dataTypeNameTyped)
                        {
                            dataTypeName = dataTypeNameTyped;
                        }

                        var scale = (short?)null;
                        if (properties.TryGetValue("NumericScale", out v) && v is short scaleType)
                        {
                            scale = scaleType;

                            if (ConnectionString.SqlEngine is AdoNetEngine.MsSql &&
                                (string.Equals(dataTypeName, "money", StringComparison.InvariantCultureIgnoreCase)
                                || string.Equals(dataTypeName, "smallmoney", StringComparison.InvariantCultureIgnoreCase)))
                            {
                                scale = 4;
                            }
                        }

                        if (scale == 255)
                            scale = null;

                        var clrType = reader.GetFieldType(i);
                        var hasPrecisionOrScale = clrType == typeof(decimal) || clrType == typeof(double) || clrType == typeof(float);

                        var info = new DataTypeInfo()
                        {
                            Name = columns[i].NameInRow,
                            ClrType = reader.GetFieldType(i),
                            ClrTypeName = reader.GetFieldType(i).Name,
                            DataTypeName = dataTypeName,
                            AllowNull = properties.TryGetValue("AllowDBNull", out v) && v is bool bv ? bv : null,
                            Precision = hasPrecisionOrScale && properties.TryGetValue("NumericPrecision", out v) && v is short sv ? sv : null,
                            Scale = hasPrecisionOrScale ? scale : null,
                            Size = properties.TryGetValue("ColumnSize", out v) && v is int iv ? iv : null,
                            IsUnique = properties.TryGetValue("IsUnique", out v) && v is bool bv2 && bv2,
                            IsKey = properties.TryGetValue("IsKey", out v) && v is bool bv3 && bv3,
                            IsIdentity = properties.TryGetValue("IsIdentity", out v) && v is bool bv4 && bv4,
                            IsAutoIncrement = properties.TryGetValue("IsAutoIncrement", out v) && v is bool bv5 && bv5,
                            IsRowVersion = properties.TryGetValue("IsRowVersion", out v) && v is bool bv6 && bv6,
                            AllProperties = properties.ToDictionary(x => x.Key, x => x.Value.ToString()),
                        };

                        columns[i].Info = info;
                        ColumnInfoList.Add(info);
                    }
                }

                if (ColumnInfoList != null && ColumnInfoListTester != null)
                {
                    try
                    {
                        ColumnInfoListTester.Invoke(ColumnInfoList);
                    }
                    catch (Exception ex)
                    {
                        var exception = new SqlSchemaException(this, "error in schema: " + ex.Message, ex);
                        exception.Data["ConnectionStringName"] = ConnectionString.Name;
                        exception.Data["Statement"] = cmd.CommandText;
                        exception.Data["ColumnNames"] = string.Join(',', ColumnInfoList.Select(x => x.Name));
                        ioCommand.Failed(exception);
                        throw exception;
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

                        ioCommand.AffectedDataCount += resultCount;
                        ioCommand.Failed(exception);
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
                                value = processor.ProcessValue(value, column.Info);
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
            ioCommand.End();
        }
        finally
        {
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
        public DataTypeInfo Info { get; set; }
    }

    protected abstract IoCommand RegisterIoCommand(string transactionId, int timeout, string statement);
    protected abstract string CreateSqlStatement();
}
