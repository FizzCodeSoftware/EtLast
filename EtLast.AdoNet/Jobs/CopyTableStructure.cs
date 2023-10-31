namespace FizzCode.EtLast;

public sealed class CopyTableStructure : AbstractSqlStatements
{
    [ProcessParameterNullException]
    public required List<TableCopyConfiguration> Configuration { get; init; }

    public CopyTableStructure(IEtlContext context)
        : base(context)
    {
    }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        var statements = new List<string>();
        var sb = new StringBuilder();

        foreach (var config in Configuration)
        {
            var columnList = (config.Columns == null || config.Columns.Count == 0)
                ? "*"
                 : string.Join(", ", config.Columns.Select(column => (column.Value ?? column.Key) + (column.Value != null ? " AS " + column.Key : "")));

            var dropTableStatement = (ConnectionString.SqlEngine, ConnectionString.Version) switch
            {
                (SqlEngine.MsSql, "2005" or "2008" or "2008 R2" or "2008R2" or "2012" or "2014")
                    => "IF OBJECT_ID('" + config.TargetTableName + "', 'U') IS NOT NULL DROP TABLE " + config.TargetTableName,
                _ => "DROP TABLE IF EXISTS " + config.TargetTableName,
            };

            sb.Append(dropTableStatement)
                .Append("; SELECT ")
                .Append(columnList)
                .Append(" INTO ")
                .Append(config.TargetTableName)
                .Append(" FROM ")
                .Append(config.SourceTableName)
                .AppendLine(" WHERE 1=0");

            statements.Add(sb.ToString());
            sb.Clear();
        }

        return statements;
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var config = Configuration[statementIndex];
        var iocUid = Context.RegisterIoCommandStartWithPath(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(config.TargetTableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "create new table based on table", ConnectionString.Unescape(config.SourceTableName));

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "copy table structure", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to create table, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                ConnectionString.Name, ConnectionString.Unescape(config.SourceTableName), ConnectionString.Unescape(config.TargetTableName), config.Columns != null ? string.Join(",", config.Columns.Select(column => column.Value ?? column.Key)) : "all", ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["SourceTableName"] = ConnectionString.Unescape(config.SourceTableName);
            exception.Data["TargetTableName"] = ConnectionString.Unescape(config.TargetTableName);
            if (config.Columns != null)
            {
                exception.Data["SourceColumns"] = string.Join(",", config.Columns.Select(column => ConnectionString.Unescape(column.Value ?? column.Key)));
            }

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

        Context.Log(transactionId, LogSeverity.Debug, this, "{TableCount} table(s) successfully created on {ConnectionStringName}", lastSucceededIndex + 1,
            ConnectionString.Name);
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CopyTableStructureFluent
{
    public static IFlow CopyTableStructure(this IFlow builder, Func<CopyTableStructure> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}