namespace FizzCode.EtLast;

public sealed class MsSqlEnableConstraintCheckFiltered : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public List<KeyValuePair<string, List<string>>> ConstraintNames { get; init; }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return ConstraintNames.ConvertAll(kvp => "ALTER TABLE " + kvp.Key + " WITH CHECK CHECK CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var tableName = ConstraintNames[statementIndex].Key;
        var ioCommandId = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "enable constraint check", null);

        try
        {
            command.ExecuteNonQuery();
            var time = startedOn.Elapsed;

            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, ioCommandId, null);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "enable constraint check", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to enable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, tableName, ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["TableName"] = ConnectionString.Unescape(tableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = startedOn.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, ioCommandId, null, exception);
            throw exception;
        }
    }

    protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
    {
        if (lastSucceededIndex == -1)
            return;

        Context.Log(transactionId, LogSeverity.Debug, this, "constraint check successfully enabled on {TableCount} tables on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}
