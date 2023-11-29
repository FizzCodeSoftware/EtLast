namespace FizzCode.EtLast;

public sealed class GetTableRecordCount : AbstractSqlStatementWithResult<int>
{
    public required string TableName { get; init; }

    /// <summary>
    /// Set to null to get the count of all records in the table.
    /// </summary>
    public required string WhereClause { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override void ValidateImpl()
    {
        base.ValidateImpl();

        if (string.IsNullOrEmpty(TableName))
            throw new ProcessParameterNullException(this, nameof(TableName));
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return string.IsNullOrEmpty(WhereClause)
            ? "SELECT COUNT(*) FROM " + TableName
            : "SELECT COUNT(*) FROM " + TableName + " WHERE " + WhereClause;
    }

    protected override int RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommandId = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbReadCount, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "getting record count", null);

        try
        {
            var result = command.ExecuteScalar();
            if (result is not int recordCount)
                recordCount = 0;

            Context.Log(transactionId, LogSeverity.Debug, this, "record count in {ConnectionStringName}/{TableName} is {RecordCount}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), recordCount);

            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, ioCommandId, recordCount);
            return recordCount;
        }
        catch (Exception ex)
        {
            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table record count query failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadCount, ioCommandId, null, exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class GetTableRecordCountFluent
{
    public static IFlow GetTableRecordCount(this IFlow builder, out int recordCount, Func<GetTableRecordCount> processCreator)
    {
        return builder.ExecuteProcessWithResult(out recordCount, processCreator);
    }
}