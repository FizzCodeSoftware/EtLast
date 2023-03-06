﻿namespace FizzCode.EtLast;

public sealed class CustomSqlAdoNetDbReaderOld : AbstractAdoNetDbReaderOld
{
    public string Sql { get; init; }
    public string MainTableName { get; init; }

    public CustomSqlAdoNetDbReaderOld(IEtlContext context)
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
                "querying from {ConnectionStringName}/{TableName} using custom query",
                ConnectionString.Name, ConnectionString.Unescape(MainTableName));
        }
        else
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "querying from {ConnectionStringName} using custom query",
                ConnectionString.Name);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomSqlAdoNetDbReaderOldFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromCustomSqlOld(this IFluentSequenceBuilder builder, CustomSqlAdoNetDbReaderOld reader)
    {
        return builder.ReadFrom(reader);
    }
}