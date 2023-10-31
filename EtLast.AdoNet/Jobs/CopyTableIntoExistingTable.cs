namespace FizzCode.EtLast;

public sealed class CopyTableIntoExistingTable : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public required TableCopyConfiguration Configuration { get; init; }

    /// <summary>
    /// Optional. Default is NULL which means everything will be transferred from the source table to the target table.
    /// </summary>
    public string WhereClause { get; init; }

    /// <summary>
    /// Default value is false.
    /// </summary>
    public bool CopyIdentityColumns { get; init; }

    public Dictionary<string, object> ColumnDefaults { get; init; }

    public CopyTableIntoExistingTable(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return Configuration?.TargetTableName != null
            ? ConnectionString?.Unescape(Configuration.TargetTableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        var statement = "";
        if (CopyIdentityColumns && ConnectionString.SqlEngine == SqlEngine.MsSql)
        {
            if (Configuration.Columns == null || Configuration.Columns.Count == 0)
                throw new InvalidProcessParameterException(this, nameof(Configuration) + "." + nameof(TableCopyConfiguration.Columns), null, "identity columns can be copied only if the column list is specified");

            statement = "SET IDENTITY_INSERT " + Configuration.TargetTableName + " ON; ";
        }

        if (Configuration.Columns == null || Configuration.Columns.Count == 0)
        {
            statement += "INSERT INTO " + Configuration.TargetTableName + " SELECT * FROM " + Configuration.SourceTableName;
        }
        else
        {
            var sourceColumnList = string.Join(", ", Configuration.Columns.Select(column => column.Value ?? column.Key));
            var targetColumnList = string.Join(", ", Configuration.Columns.Select(column => column.Key));

            if (ColumnDefaults != null)
            {
                var index = 0;
                foreach (var kvp in ColumnDefaults)
                {
                    var paramName = "_" + ConnectionString.Unescape(kvp.Key);
                    sourceColumnList += ", @" + paramName + " as " + kvp.Key;
                    targetColumnList += ", " + kvp.Key;
                    parameters.Add(paramName, kvp.Value ?? DBNull.Value);
                    index++;
                }
            }

            statement += "INSERT INTO " + Configuration.TargetTableName + " (" + targetColumnList + ") SELECT " + sourceColumnList + " FROM " + Configuration.SourceTableName;
        }

        if (WhereClause != null)
        {
            statement += " WHERE " + WhereClause.Trim();
        }

        if (CopyIdentityColumns && ConnectionString.SqlEngine == SqlEngine.MsSql)
        {
            statement += "; SET IDENTITY_INSERT " + Configuration.TargetTableName + " OFF; ";
        }

        return statement;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbWriteCopy, ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "copying records from table", ConnectionString.Unescape(Configuration.SourceTableName));

        try
        {
            var recordCount = command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteCopy, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "copy table into existing", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                ConnectionString.Name, ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName),
                Configuration.Columns != null
                    ? string.Join(",", Configuration.Columns.Select(column => column.Value ?? column.Key))
                    : "all",
                ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["SourceTableName"] = ConnectionString.Unescape(Configuration.SourceTableName);
            exception.Data["TargetTableName"] = ConnectionString.Unescape(Configuration.TargetTableName);
            if (Configuration.Columns != null)
            {
                exception.Data["SourceColumns"] = string.Join(",", Configuration.Columns.Select(column => ConnectionString.Unescape(column.Value ?? column.Key)));
            }

            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteCopy, iocUid, null, exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CopyTableIntoExistingTableFluent
{
    public static IFlow CopyTableIntoExistingTable(this IFlow builder, Func<CopyTableIntoExistingTable> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}