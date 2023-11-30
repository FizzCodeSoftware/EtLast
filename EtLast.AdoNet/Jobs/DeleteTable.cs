namespace FizzCode.EtLast;

public sealed class DeleteTable : AbstractSqlStatement
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
        var ioCommand = Context.RegisterIoCommandStart(this, new IoCommand()
        {
            Kind = IoCommandKind.dbDelete,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "deleting records",
        });

        try
        {
            var recordCount = command.ExecuteNonQuery();
            ioCommand.AffectedDataCount += recordCount;
            Context.RegisterIoCommandEnd(this, ioCommand);
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

            ioCommand.Exception = exception;
            Context.RegisterIoCommandEnd(this, ioCommand);
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