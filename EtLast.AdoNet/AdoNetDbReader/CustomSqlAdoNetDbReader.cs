﻿namespace FizzCode.EtLast;

public sealed class CustomSqlAdoNetDbReader : AbstractAdoNetDbReader
{
    [ProcessParameterMustHaveValue]
    public required string Sql { get; init; }

    public required string MainTableName { get; init; }

    protected override CommandType GetCommandType() => CommandType.Text;

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
            Message = "read from custom query"
        });
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomSqlAdoNetDbReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromCustomSql(this IFluentSequenceBuilder builder, CustomSqlAdoNetDbReader reader)
    {
        return builder.ReadFrom(reader);
    }
}