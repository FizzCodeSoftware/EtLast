﻿namespace FizzCode.EtLast;

public sealed class GetTableRecordCount : AbstractSqlStatementWithResult<int>
{
    [ProcessParameterMustHaveValue] public required string TableName { get; init; }

    /// <summary>
    /// Set to null to get the count of all records in the table.
    /// </summary>
    public required string WhereClause { get; init; }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return string.IsNullOrEmpty(WhereClause)
            ? "SELECT COUNT(*) FROM " + TableName
            : "SELECT COUNT(*) FROM " + TableName + " WHERE " + WhereClause;
    }

    protected override int RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbReadCount,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "getting record count",
        });

        try
        {
            var result = command.ExecuteScalar();
            if (result is not int recordCount)
                recordCount = 0;

            Context.Log(transactionId, LogSeverity.Debug, this, "record count in {ConnectionStringName}/{TableName} is {RecordCount}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), recordCount);

            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
            return recordCount;
        }
        catch (Exception ex)
        {
            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table record count query failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = ExecutionInfo.Timer.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class GetTableRecordCountFluent
{
    public static IFlow GetTableRecordCount(this IFlow builder, out int recordCount, Func<GetTableRecordCount> processCreator)
    {
        return builder.ExecuteProcessWithResult(out recordCount, processCreator);
    }
}