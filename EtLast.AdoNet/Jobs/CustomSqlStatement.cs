namespace FizzCode.EtLast;

public sealed class CustomSqlStatement : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public required string SqlStatement { get; init; }

    public required string MainTableName { get; init; }

    public Dictionary<string, object> Parameters { get; init; }

    /// <summary>
    /// Some SQL connector implementations does not support passing arrays due to parameters (like MySQL).
    /// If set to true, then all int[], long[], List&lt;int&gt; and List&lt;long&gt; parameters will be converted to a comma separated list and inlined into the SQL statement right before execution.
    /// Default value is true.
    /// </summary>
    public bool InlineArrayParameters { get; init; } = true;

    public override string GetTopic()
    {
        return MainTableName != null
            ? ConnectionString?.Unescape(MainTableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        if (Parameters != null)
        {
            foreach (var p in Parameters)
                parameters.Add(p.Key, p.Value);
        }

        var sqlStatementProcessed = InlineArrayParametersIfNecessary(SqlStatement);
        return sqlStatementProcessed;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbRead,
            Location = ConnectionString.Name,
            Path = MainTableName,
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "custom SQL statement",
        });

        try
        {
            var recordCount = command.ExecuteNonQuery();
            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlStatementException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "custom SQL statement failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }

    private string InlineArrayParametersIfNecessary(string sqlStatement)
    {
        if (InlineArrayParameters && Parameters != null)
        {
            var parameters = Parameters.ToList();
            foreach (var kvp in parameters)
            {
                var paramReference = "@" + kvp.Key;
                var idx = sqlStatement.IndexOf(paramReference, StringComparison.InvariantCultureIgnoreCase);
                if (idx == -1)
                    continue;

                if (kvp.Value is int[] intArray)
                {
                    var newParamText = string.Join(",", intArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    Parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is long[] longArray)
                {
                    var newParamText = string.Join(",", longArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    Parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is List<int> intList)
                {
                    var newParamText = string.Join(",", intList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    Parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is List<long> longList)
                {
                    var newParamText = string.Join(",", longList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    Parameters.Remove(kvp.Key);
                }
            }
        }

        return sqlStatement;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomSqlStatementFluent
{
    public static IFlow CustomSqlStatement(this IFlow builder, Func<CustomSqlStatement> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}