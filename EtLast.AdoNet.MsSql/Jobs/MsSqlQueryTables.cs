namespace FizzCode.EtLast;

public sealed class MsSqlQueryTables : AbstractSqlStatementWithResult<List<MsSqlTableName>>
{
    public string SchemaName { get; init; }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (ConnectionString.GetAdoNetEngine() != AdoNetEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        var statement = "select * from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE'";
        if (!string.IsNullOrEmpty(SchemaName))
        {
            statement += " and TABLE_SCHEMA = @schemaName";
            parameters.Add("schemaName", SchemaName);
        }

        return statement;
    }

    protected override List<MsSqlTableName> RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var tableNames = new List<MsSqlTableName>();

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbRead,
            Location = ConnectionString.Name,
            Path = "INFORMATION_SCHEMA.TABLES",
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "querying table names",
        });

        try
        {
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var schema = (string)reader["TABLE_SCHEMA"];
                    var tableName = (string)reader["TABLE_NAME"];

                    tableNames.Add(new MsSqlTableName()
                    {
                        Schema = schema,
                        TableName = tableName,
                        EscapedName = ConnectionString.Escape(tableName, schema)
                    });
                }

                ioCommand.AffectedDataCount += tableNames.Count;
                ioCommand.End();
            }
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaReadException(this, "table names", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "table list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.InvocationStarted.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }

        return tableNames;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MsSqlQueryTablesFluent
{
    public static IFlow MsSqlQueryTables(this IFlow builder, out List<MsSqlTableName> tableNames, Func<MsSqlQueryTables> processCreator)
    {
        return builder.ExecuteProcessWithResult(out tableNames, processCreator);
    }
}