namespace FizzCode.EtLast;

public sealed class MsSqlResetSingleIdentityCounter : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public string TableName { get; init; }

    [ProcessParameterMustHaveValue]
    public string IdentityColumnName { get; init; }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.GetSqlEngine() != AdoNetEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "declare @max int select @max=ISNULL(max(" + IdentityColumnName + "),0) from " + TableName + "; DBCC CHECKIDENT ('" + TableName + "', RESEED, @max);";
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbIdentityReset,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(TableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "resetting identity counter on column",
            MessageExtra = IdentityColumnName,
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlIdentityResetException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "identity counter reset failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["Table"] = TableName;
            exception.Data["IdentityColumn"] = IdentityColumnName;
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}
