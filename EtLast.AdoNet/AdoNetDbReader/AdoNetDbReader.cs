﻿namespace FizzCode.EtLast;

public sealed class AdoNetDbReader : AbstractAdoNetDbReader
{
    [ProcessParameterMustHaveValue]
    public required string TableName { get; init; }

    public string CustomWhereClause { get; init; }
    public string CustomOrderByClause { get; init; }
    public int RecordCountLimit { get; init; }

    protected override CommandType GetCommandType() => CommandType.Text;

    protected override string CreateSqlStatement()
    {
        var columnList = "*";
        if (Columns?.Count > 0)
        {
            columnList = string.Join(", ", Columns.Select(x => ConnectionString.Escape(x.Value?.SourceColumn ?? x.Key)));
        }

        var prefix = "";
        var postfix = "";

        if (!string.IsNullOrEmpty(CustomWhereClause))
        {
            postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "WHERE " + CustomWhereClause;
        }

        if (!string.IsNullOrEmpty(CustomOrderByClause))
        {
            postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "ORDER BY " + CustomOrderByClause;
        }

        if (RecordCountLimit > 0)
        {
            if (ConnectionString.SqlEngine == AdoNetEngine.MySql)
            {
                postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "LIMIT " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
            }
            else
            {
                prefix = "TOP " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
            }
            // todo: support Oracle Syntax: https://www.w3schools.com/sql/sql_top.asp
        }

        return "SELECT "
            + (!string.IsNullOrEmpty(prefix) ? prefix + " " : "")
            + columnList
            + " FROM "
            + TableName
            + (!string.IsNullOrEmpty(postfix) ? " " + postfix : "");
    }

    protected override IoCommand RegisterIoCommand(string transactionId, int timeout, string statement)
    {
        return Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbRead,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = timeout,
            Command = statement,
            TransactionId = transactionId,
            ArgumentListGetter = () => Parameters,
            Message = "read from table"
        });
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class AdoNetDbReaderFluent
{
    public static IFluentSequenceMutatorBuilder ReadFromSql(this IFluentSequenceBuilder builder, AdoNetDbReader reader)
    {
        return builder.ReadFrom(reader);
    }
}