namespace FizzCode.EtLast;

public sealed class CreatePrimaryKeyConstraint : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public required string TableName { get; init; }

    [ProcessParameterMustHaveValue]
    public required string ConstraintName { get; init; }

    [ProcessParameterMustHaveValue]
    public required string[] Columns { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "ALTER TABLE " + TableName + " ADD CONSTRAINT " + ConstraintName + " PRIMARY KEY (" + string.Join(',', Columns) + ")";
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommandStart(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "creating primary key constraint",
        });

        try
        {
            var recordCount = command.ExecuteNonQuery();
            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlDeleteException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "primary key constraint creation failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CreatePrimaryKeyConstraintFluent
{
    public static IFlow CreatePrimaryKeyConstraint(this IFlow builder, Func<CreatePrimaryKeyConstraint> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}