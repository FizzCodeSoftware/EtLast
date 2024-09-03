namespace FizzCode.EtLast;

public sealed class MsSqlDropSchemas : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public required string[] SchemaNames { get; init; }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.SqlEngine != AdoNetEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override List<string> CreateSqlStatements(INamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return SchemaNames
            .Where(x => !string.Equals(x, "dbo", StringComparison.InvariantCultureIgnoreCase))
            .Select(x => "DROP SCHEMA IF EXISTS " + x + ";")
            .ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var schemaName = SchemaNames[statementIndex];

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(schemaName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "drop schema",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "drop schema", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop schema, connection string key: {0}, schema: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(schemaName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["SchemaName"] = ConnectionString.Unescape(schemaName);
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

        Context.Log(transactionId, LogSeverity.Debug, this, "{SchemaCount} schema(s) successfully dropped on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}
