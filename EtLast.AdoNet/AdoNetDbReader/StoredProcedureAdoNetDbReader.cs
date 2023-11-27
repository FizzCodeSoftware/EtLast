namespace FizzCode.EtLast;

public sealed class StoredProcedureAdoNetDbReader : AbstractAdoNetDbReader
{
    [ProcessParameterMustHaveValue]
    public required string Sql { get; init; }

    /// <summary>
    /// The name the SP is referred in the logs.
    /// </summary>
    public required string MainTableName { get; init; }

    protected override CommandType GetCommandType() => CommandType.StoredProcedure;

    public override string GetTopic() => MainTableName != null
        ? ConnectionString?.Unescape(MainTableName)
        : null;

    protected override string CreateSqlStatement()
    {
        return Sql;
    }

    protected override long RegisterIoCommandStart(string transactionId, int timeout, string statement)
    {
        if (MainTableName != null)
        {
            return Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbRead, ConnectionString.Name, ConnectionString.Unescape(MainTableName), timeout, statement, transactionId, () => Parameters,
                "read from stored procedure", null);
        }
        else
        {
            return Context.RegisterIoCommandStartWithLocation(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "read from stored procedure", null);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class StoredProcedureAdoNetDbReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromStoredProcedure(this IFluentSequenceBuilder builder, StoredProcedureAdoNetDbReader reader)
    {
        return builder.ReadFrom(reader);
    }
}