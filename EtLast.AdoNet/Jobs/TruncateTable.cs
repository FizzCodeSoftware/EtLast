namespace FizzCode.EtLast;

public sealed class TruncateTable : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public string TableName { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "TRUNCATE TABLE " + TableName;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var originalStatement = command.CommandText;

        var recordCount = 0;
        command.CommandText = "SELECT COUNT(*) FROM " + TableName;
        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbReadCount, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "querying record count", null);

        try
        {
            recordCount = (int)command.ExecuteScalar();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadCount, iocUid, null, exception);
            throw exception;
        }

        command.CommandText = originalStatement;
        iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbDelete, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "truncating table", null);

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbDelete, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            var exception = new SqlTruncateException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = originalStatement;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbDelete, iocUid, null, exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class TruncateTableFluent
{
    public static IFlow TruncateTable(this IFlow builder, Func<TruncateTable> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}