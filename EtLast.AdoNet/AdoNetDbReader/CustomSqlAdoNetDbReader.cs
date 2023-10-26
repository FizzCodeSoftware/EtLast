namespace FizzCode.EtLast;

public sealed class CustomSqlAdoNetDbReader : AbstractAdoNetDbReader
{
    public required string Sql { get; init; }
    public required string MainTableName { get; init; }

    public CustomSqlAdoNetDbReader(IEtlContext context)
        : base(context)
    {
    }

    protected override CommandType GetCommandType()
    {
        return CommandType.Text;
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
                "custom query");
        }
        else
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "custom query");
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomSqlAdoNetDbReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromCustomSql(this IFluentSequenceBuilder builder, CustomSqlAdoNetDbReader reader)
    {
        return builder.ReadFrom(reader);
    }
}
