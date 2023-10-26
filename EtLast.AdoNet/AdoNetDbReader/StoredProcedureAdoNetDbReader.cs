namespace FizzCode.EtLast;

public sealed class StoredProcedureAdoNetDbReader : AbstractAdoNetDbReader
{
    public required string Sql { get; init; }

    /// <summary>
    /// The name the SP is referred in the logs.
    /// </summary>
    public required string MainTableName { get; init; }

    public StoredProcedureAdoNetDbReader(IEtlContext context)
        : base(context)
    {
    }

    protected override CommandType GetCommandType()
    {
        return CommandType.StoredProcedure;
    }

    public override string GetTopic()
    {
        return MainTableName != null
            ? ConnectionString?.Unescape(MainTableName)
            : null;
    }

    protected override void ValidateImpl()
    {
        base.ValidateImpl();

        if (string.IsNullOrEmpty(Sql))
            throw new ProcessParameterNullException(this, nameof(Sql));
    }

    protected override string CreateSqlStatement()
    {
        return Sql;
    }

    protected override int RegisterIoCommandStart(string transactionId, int timeout, string statement)
    {
        if (MainTableName != null)
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, ConnectionString.Unescape(MainTableName), timeout, statement, transactionId, () => Parameters,
                "read from stored procedure");
        }
        else
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "read from stored procedure");
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
