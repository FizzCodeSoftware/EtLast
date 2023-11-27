namespace FizzCode.EtLast;

public sealed class CustomSqlAdoNetDbReader(IEtlContext context) : AbstractAdoNetDbReader(context)
{
    [ProcessParameterMustHaveValue]
    public required string Sql { get; init; }

    public required string MainTableName { get; init; }

    protected override CommandType GetCommandType() => CommandType.Text;

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
                "custom query", null);
        }
        else
        {
            return Context.RegisterIoCommandStartWithLocation(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "custom query", null);
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