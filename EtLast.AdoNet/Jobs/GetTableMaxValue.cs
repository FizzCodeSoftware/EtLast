namespace FizzCode.EtLast;

public sealed class GetTableMaxValue<TResult> : AbstractSqlStatementWithResult<TableMaxValueResult<TResult>>
{
    [ProcessParameterMustHaveValue] public required string TableName { get; init; }
    [ProcessParameterMustHaveValue] public required string ColumnName { get; init; }

    /// <summary>
    /// Set to null to get the max value of all records in the column.
    /// </summary>
    public string WhereClause { get; init; }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return string.IsNullOrEmpty(WhereClause)
            ? "SELECT MAX(" + ColumnName + ") AS maxValue, COUNT(*) AS cnt FROM " + TableName
            : "SELECT MAX(" + ColumnName + ") AS maxValue, COUNT(*) AS cnt FROM " + TableName + " WHERE " + WhereClause;
    }

    protected override TableMaxValueResult<TResult> RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbReadAggregate,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "getting max value from table",
        });

        try
        {
            var result = new TableMaxValueResult<TResult>();
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var mv = reader["maxValue"];
                    if (mv is not DBNull)
                    {
                        result.MaxValue = (TResult)mv;
                    }

                    result.RecordCount = (int)reader["cnt"];
                }
            }

            ioCommand.AffectedDataCount += result.RecordCount;
            ioCommand.End();
            return result;
        }
        catch (Exception ex)
        {
            var exception = new SqlAggregateReadException(this, ex, "max value");
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table max value query failed, connection string key: {0}, table: {1}, column: {2}, message: {3}, command: {4}, timeout: {5}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ConnectionString.Unescape(ColumnName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["ColumnName"] = ConnectionString.Unescape(ColumnName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = ExecutionInfo.Timer.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

public class TableMaxValueResult<T>
{
    public T MaxValue { get; set; }
    public int RecordCount { get; set; }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class GetTableMaxValueFluent
{
    public static IFlow GetTableMaxValue<TResult>(this IFlow builder, out TableMaxValueResult<TResult> maxValue, Func<GetTableMaxValue<TResult>> processCreator)
    {
        return builder.ExecuteProcessWithResult(out maxValue, processCreator);
    }
}