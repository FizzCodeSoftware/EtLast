namespace FizzCode.EtLast;

public sealed class CreatePrimaryKeyConstraint : AbstractSqlStatement
{
    public string TableName { get; init; }
    public string ConstraintName { get; init; }
    public string[] Columns { get; init; }

    public CreatePrimaryKeyConstraint(IEtlContext context)
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

        if (string.IsNullOrEmpty(ConstraintName))
            throw new ProcessParameterNullException(this, nameof(ConstraintName));

        if (Columns == null || Columns.Length == 0)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "ALTER TABLE " + TableName + " ADD CONSTRAINT " + ConstraintName + " PRIMARY KEY (" + string.Join(',', Columns) + ")";
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbDelete, ConnectionString.Name, ConnectionString.Unescape(TableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "creating primary key constraint on {ConnectionStringName}/{TableName}",
            ConnectionString.Name, ConnectionString.Unescape(TableName));

        try
        {
            var recordCount = command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbDelete, iocUid, recordCount);
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, IoCommandKind.dbDelete, iocUid, null, ex);

            var exception = new SqlDeleteException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "primary key constraint creation failed, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), ex.Message, command.CommandText, CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(TableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.LastInvocationStarted.Elapsed;
            throw exception;
        }
    }
}