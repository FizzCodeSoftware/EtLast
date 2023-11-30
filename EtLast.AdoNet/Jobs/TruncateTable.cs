namespace FizzCode.EtLast;

public sealed class TruncateTable : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public string TableName { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "TRUNCATE TABLE " + TableName;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var originalStatement = command.CommandText;

        var recordCount = 0;
        command.CommandText = "SELECT COUNT(*) FROM " + TableName;

        var ioCommand = Context.RegisterIoCommandStart(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbReadCount,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "getting record count",
        });

        try
        {
            recordCount = (int)command.ExecuteScalar();
            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }

        command.CommandText = originalStatement;

        ioCommand = Context.RegisterIoCommandStart(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbDelete,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            ArgumentListGetter = () => parameters,
            TransactionId = transactionId,
            Message = "truncating table",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlTruncateException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = originalStatement;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class TruncateTableFluent
{
    public static IFlow TruncateTable(this IFlow builder, Func<TruncateTable> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}