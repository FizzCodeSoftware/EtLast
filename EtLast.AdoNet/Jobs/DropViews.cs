namespace FizzCode.EtLast;

public sealed class DropViews : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public required string[] TableNames { get; init; }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.GetSqlEngine() is not AdoNetEngine.MsSql and not AdoNetEngine.MySql)
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
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(viewName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "drop view",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
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

            ioCommand.Failed(exception);
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

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DropViewsFluent
{
    public static IFlow DropViews(this IFlow builder, Func<DropViews> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}