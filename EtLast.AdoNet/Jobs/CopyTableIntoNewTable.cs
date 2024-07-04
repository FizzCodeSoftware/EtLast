namespace FizzCode.EtLast;

public sealed class CopyTableIntoNewTable : AbstractSqlStatement
{
    [ProcessParameterMustHaveValue]
    public required TableCopyConfiguration Configuration { get; init; }

    /// <summary>
    /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
    /// </summary>
    public string WhereClause { get; init; }

    public override string GetTopic()
    {
        return Configuration?.TargetTableName != null
            ? ConnectionString?.Unescape(Configuration.TargetTableName)
            : null;
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        var columnList = (Configuration.Columns == null || Configuration.Columns.Count == 0)
             ? "*"
             : string.Join(", ", Configuration.Columns.Select(column => (column.Value ?? column.Key) + (column.Value != null ? " AS " + column.Key : "")));

        var dropTableStatement = (ConnectionString.GetAdoNetEngine(), ConnectionString.Version) switch
        {
            (AdoNetEngine.MsSql, "2005" or "2008" or "2008 R2" or "2008R2" or "2012" or "2014")
                => "IF OBJECT_ID('" + Configuration.TargetTableName + "', 'U') IS NOT NULL DROP TABLE " + Configuration.TargetTableName,
            _ => "DROP TABLE IF EXISTS " + Configuration.TargetTableName,
        };

        var statement = dropTableStatement + "; SELECT " + columnList + " INTO " + Configuration.TargetTableName + " FROM " + Configuration.SourceTableName;

        if (WhereClause != null)
        {
            statement += " WHERE " + WhereClause.Trim();
        }

        return statement;
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbWriteCopy,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(Configuration.TargetTableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            ArgumentListGetter = () => parameters,
            Message = "creating new table and copying records from table",
            MessageExtra = ConnectionString.Unescape(Configuration.SourceTableName),
        });

        try
        {
            var recordCount = command.ExecuteNonQuery();

            ioCommand.AffectedDataCount += recordCount;
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "copy table into new", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table creation and copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                ConnectionString.Name, ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName),
                Configuration.Columns != null
                    ? string.Join(",", Configuration.Columns.Select(column => column.Value ?? column.Key))
                    : "all",
                ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["SourceTableName"] = ConnectionString.Unescape(Configuration.SourceTableName);
            exception.Data["TargetTableName"] = ConnectionString.Unescape(Configuration.TargetTableName);
            if (Configuration.Columns != null)
            {
                exception.Data["SourceColumns"] = string.Join(",", Configuration.Columns.Select(column => ConnectionString.Unescape(column.Value ?? column.Key)));
            }

            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = ExecutionInfo.Timer.Elapsed;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CopyTableIntoNewTableFluent
{
    public static IFlow CopyTableIntoNewTable(this IFlow builder, Func<CopyTableIntoNewTable> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}