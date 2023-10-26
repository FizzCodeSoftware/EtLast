namespace FizzCode.EtLast;

public sealed class MsSqlDisableConstraintCheck : AbstractSqlStatements
{
    public required string[] TableNames { get; init; }

    public MsSqlDisableConstraintCheck(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return ConnectionString != null && TableNames?.Length > 0
            ? string.Join(",", TableNames.Select(ConnectionString.Unescape))
            : null;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (TableNames == null || TableNames.Length == 0)
            throw new ProcessParameterNullException(this, nameof(TableNames));
    }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return TableNames.Select(tableName => "ALTER TABLE " + tableName + " NOCHECK CONSTRAINT ALL;").ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var tableName = TableNames[statementIndex];
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "disable constraint check");

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "disable constraint check", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to disable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(tableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = startedOn.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, exception);
            throw exception;
        }
    }

    protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
    {
        if (lastSucceededIndex == -1)
            return;

        Context.Log(transactionId, LogSeverity.Debug, this, "constraint check successfully disabled on {TableCount} tables on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}
