namespace FizzCode.EtLast;

public sealed class DeleteTable(IEtlContext context) : AbstractSqlStatement(context)
{
    [ProcessParameterMustHaveValue]
    public required string TableName { get; init; }

    /// <summary>
    /// Set to null for a full table delete, but consider using <see cref="TruncateTable"/> if the target table has no FK references.
    /// </summary>
    public required string WhereClause { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return string.IsNullOrEmpty(WhereClause)
            ? "DELETE FROM " + TableName
            : "DELETE FROM " + TableName + " WHERE " + WhereClause;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbDelete, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "deleting records", null);

        try
        {
            var recordCount = command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbDelete, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            var exception = new SqlDeleteException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table content deletion failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbDelete, iocUid, null, exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DeleteTableFluent
{
    public static IFlow DeleteTable(this IFlow builder, Func<DeleteTable> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}