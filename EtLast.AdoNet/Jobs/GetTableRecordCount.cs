namespace FizzCode.EtLast;

public sealed class GetTableRecordCount : AbstractSqlStatementWithResult<int>
{
    public string TableName { get; init; }
    public string CustomWhereClause { get; init; }

    public GetTableRecordCount(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    protected override void ValidateImpl()
    {
        base.ValidateImpl();

        if (string.IsNullOrEmpty(TableName))
            throw new ProcessParameterNullException(this, nameof(TableName));
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return string.IsNullOrEmpty(CustomWhereClause)
            ? "SELECT COUNT(*) FROM " + TableName
            : "SELECT COUNT(*) FROM " + TableName + " WHERE " + CustomWhereClause;
    }

    protected override int RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadCount, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "getting record count from {ConnectionStringName}/{TableName}",
            ConnectionString.Name, ConnectionString.Unescape(TableName));

        try
        {
            var result = command.ExecuteScalar();
            if (result is not int recordCount)
                recordCount = 0;

            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, iocUid, recordCount);
            return recordCount;
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, IoCommandKind.dbReadCount, iocUid, null, ex);

            var exception = new SqlRecordCountReadException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table record count query failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
            exception.Data.Add("TableName", ConnectionString.Unescape(TableName));
            exception.Data.Add("Statement", command.CommandText);
            exception.Data.Add("Timeout", CommandTimeout);
            exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
            throw exception;
        }
    }
}
