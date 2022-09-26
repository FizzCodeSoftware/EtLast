namespace FizzCode.EtLast;

public sealed class TruncateTable : AbstractSqlStatement
{
    public string TableName { get; init; }

    public TruncateTable(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (string.IsNullOrEmpty(TableName))
            throw new ProcessParameterNullException(this, nameof(TableName));
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
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadCount, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "querying record count from {ConnectionStringName}/{TableName}",
            ConnectionString.Name, ConnectionString.Unescape(TableName));

        try
        {
            recordCount = (int)command.ExecuteScalar();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadCount, iocUid, null, ex);

            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
            exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
            exception.Data.Add("Statement", command.CommandText);
            exception.Data.Add("Timeout", CommandTimeout);
            exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
            throw exception;
        }

        command.CommandText = originalStatement;
        iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbDelete, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "truncating {ConnectionStringName}/{TableName}",
            ConnectionString.Name, ConnectionString.Unescape(TableName));

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbDelete, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, IoCommandKind.dbDelete, iocUid, null, ex);

            var exception = new SqlTruncateException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table truncate failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, originalStatement, CommandTimeout));

            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
            exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
            exception.Data.Add("Statement", originalStatement);
            exception.Data.Add("Timeout", CommandTimeout);
            exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
            throw exception;
        }
    }
}
