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

    protected override IoCommand RegisterIoCommand(string transactionId, int timeout, string statement)
    {
        return Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbRead,
            Location = ConnectionString.Name,
            Path = MainTableName != null
                ? ConnectionString.Unescape(MainTableName)
                : null,
            TimeoutSeconds = timeout,
            Command = statement,
            TransactionId = transactionId,
            ArgumentListGetter = () => Parameters,
            Message = "read from stored procedure"
        });
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