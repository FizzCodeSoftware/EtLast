namespace FizzCode.EtLast;

public sealed class DropViews : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public required string[] TableNames { get; init; }

    public DropViews(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.SqlEngine is not SqlEngine.MsSql and not SqlEngine.MySql)
        {
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient or MySql.Data.MySqlClient");
        }
    }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return TableNames.Select(viewName => "DROP VIEW IF EXISTS " + viewName + ";").ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var viewName = TableNames[statementIndex];
        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(viewName), command.CommandTimeout, command.CommandText, transactionId, null,
            "drop view", null);

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "drop view", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop view, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(viewName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["ViewName"] = ConnectionString.Unescape(viewName);
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

        Context.Log(transactionId, LogSeverity.Debug, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DropViewsFluent
{
    public static IFlow DropViews(this IFlow builder, Func<DropViews> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}