namespace FizzCode.EtLast;

public sealed class DropTables : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public required string[] TableNames { get; init; }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return TableNames
            .Select(tableName =>
            {
                var dropTableStatement = (ConnectionString.SqlEngine, ConnectionString.Version) switch
                {
                    (SqlEngine.MsSql, "2005" or "2008" or "2008 R2" or "2008R2" or "2012" or "2014")
                        => "IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE " + tableName,
                    _ => "DROP TABLE IF EXISTS " + tableName,
                };
                return dropTableStatement + ";";
            })
            .ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var tableName = TableNames[statementIndex];
        var originalStatement = command.CommandText;

        var recordCount = 0;
        command.CommandText = "SELECT COUNT(*) FROM " + tableName;
        var ioCommand = Context.RegisterIoCommandStart(this, new IoCommand()
        {
            Kind = IoCommandKind.dbReadCount,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(tableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "querying record count",
        });

        try
        {
            recordCount = (int)command.ExecuteScalar();
            ioCommand.AffectedDataCount += recordCount;
            Context.RegisterIoCommandEnd(this, ioCommand);
        }
        catch (Exception)
        {
            Context.RegisterIoCommandEnd(this, ioCommand);
        }

        command.CommandText = originalStatement;
        ioCommand = Context.RegisterIoCommandStart(this, new IoCommand()
        {
            Kind = IoCommandKind.dbDropTable,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(tableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "drop table",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.AffectedDataCount += recordCount;
            Context.RegisterIoCommandEnd(this, ioCommand);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "drop table", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(tableName);
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = startedOn.Elapsed;

            ioCommand.Exception = exception;
            Context.RegisterIoCommandEnd(this, ioCommand);
            throw exception;
        }
    }

    protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
    {
        if (lastSucceededIndex == -1)
            return;

        Context.Log(transactionId, LogSeverity.Debug, this, "{TableCount} table(s) successfully dropped on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DropTablesFluent
{
    public static IFlow DropTables(this IFlow builder, Func<DropTables> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}