namespace FizzCode.EtLast;

public enum MsSqlDropForeignKeysProcessMode { All, InSpecifiedTables, InSpecifiedSchema, ToSpecifiedSchema, ToSpecifiedTables }

public sealed class MsSqlDropForeignKeys : AbstractSqlStatements
{
    /// <summary>
    /// Default value is <see cref="MsSqlDropForeignKeysProcessMode.ToSpecifiedTables"/>
    /// </summary>
    public required MsSqlDropForeignKeysProcessMode Mode { get; init; } = MsSqlDropForeignKeysProcessMode.ToSpecifiedTables;

    /// <summary>
    /// Table names must include schema name.
    /// </summary>
    public string[] TableNames { get; init; }

    /// <summary>
    /// Must be set if <see cref="Mode"/> is set to <see cref="MsSqlDropForeignKeysProcessMode.InSpecifiedSchema"/> or <see cref="MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema"/>
    /// </summary>
    public string SchemaName { get; init; }

    private List<Tuple<string, int>> _tableNamesAndCounts;

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        switch (Mode)
        {
            case MsSqlDropForeignKeysProcessMode.InSpecifiedTables:
                if (TableNames == null || TableNames.Length == 0)
                    throw new ProcessParameterNullException(this, nameof(TableNames));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.InSpecifiedTables));
                break;
            case MsSqlDropForeignKeysProcessMode.All:
                if (TableNames != null)
                    throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.All));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.All));
                break;
            case MsSqlDropForeignKeysProcessMode.InSpecifiedSchema:
                if (TableNames != null)
                    throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.InSpecifiedSchema));
                if (string.IsNullOrEmpty(SchemaName))
                    throw new ProcessParameterNullException(this, nameof(SchemaName));
                break;
            case MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema:
                if (TableNames != null)
                    throw new InvalidProcessParameterException(this, nameof(TableNames), TableNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema));
                if (string.IsNullOrEmpty(SchemaName))
                    throw new ProcessParameterNullException(this, nameof(SchemaName));
                break;
        }

        if (ConnectionString.SqlEngine != SqlEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        var startedOn = Stopwatch.StartNew();
        using (var command = connection.CreateCommand())
        {
            var parameters = new Dictionary<string, object>();

            command.CommandTimeout = CommandTimeout;
            switch (Mode)
            {
                case MsSqlDropForeignKeysProcessMode.ToSpecifiedSchema:
                    {
                        command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
	inner join sys.objects o on fkc.referenced_object_id = o.object_id and o.schema_id = SCHEMA_ID(@schemaName)";
                        parameters.Add("schemaName", SchemaName);
                        break;
                    }

                case MsSqlDropForeignKeysProcessMode.InSpecifiedSchema:
                    {
                        command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
where fk.schema_id = SCHEMA_ID(@schemaName)";
                        parameters.Add("schemaName", SchemaName);
                        break;
                    }

                case MsSqlDropForeignKeysProcessMode.InSpecifiedTables:
                    command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id";
                    break;
                case MsSqlDropForeignKeysProcessMode.ToSpecifiedTables:
                    command.CommandText = @"
select distinct
	fk.[name] fkName,
	SCHEMA_NAME(fk.schema_id) schemaName,
	OBJECT_NAME(fk.parent_object_id) tableName, 
	SCHEMA_NAME(o.schema_id) refSchemaName,
	o.name refTableName
from
	sys.foreign_keys fk
	inner join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
	inner join sys.objects o on fkc.referenced_object_id = o.object_id";
                    break;
            }

            // tables are not filtered with an IN clause due to the limitations of the query processor
            // this solution will read unnecessary data, but it will work in all conditions

            command.FillCommandParameters(parameters);

            var tablesNamesHashSet = Mode is MsSqlDropForeignKeysProcessMode.InSpecifiedTables or MsSqlDropForeignKeysProcessMode.ToSpecifiedTables
                ? TableNames.Select(x => x.ToUpperInvariant()).ToHashSet()
                : null;

            var constraintsByTable = new Dictionary<string, List<string>>();

            var ioCommand = Context.RegisterIoCommandStart(new IoCommand()
            {
                Process = this,
                Kind = IoCommandKind.dbReadMeta,
                Location = ConnectionString.Name,
                Path = "SYS.FOREIGN_KEYS",
                TimeoutSeconds = command.CommandTimeout,
                Command = command.CommandText,
                TransactionId = transactionId,
                Message = "querying foreign key names",
            });

            var recordsRead = 0;
            try
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        recordsRead++;
                        var sourceTableName = ConnectionString.Escape((string)reader["tableName"], (string)reader["schemaName"]);
                        var sourceTableNameUpper = sourceTableName.ToUpperInvariant();

                        if (Mode == MsSqlDropForeignKeysProcessMode.InSpecifiedTables && !tablesNamesHashSet.Contains(sourceTableNameUpper))
                            continue;

                        if (Mode == MsSqlDropForeignKeysProcessMode.ToSpecifiedTables)
                        {
                            var referredTableName = ConnectionString.Escape((string)reader["refTableName"], (string)reader["refSchemaName"]);
                            if (!tablesNamesHashSet.Contains(sourceTableNameUpper))
                                continue;
                        }

                        if (!constraintsByTable.TryGetValue(sourceTableName, out var list))
                        {
                            list = [];
                            constraintsByTable.Add(sourceTableName, list);
                        }

                        list.Add(ConnectionString.Escape((string)reader["fkName"]));
                    }

                    ioCommand.AffectedDataCount += recordsRead;
                    ioCommand.End();
                }

                _tableNamesAndCounts = [];
                var statements = new List<string>();
                foreach (var kvp in constraintsByTable.OrderBy(x => x.Key))
                {
                    _tableNamesAndCounts.Add(new Tuple<string, int>(kvp.Key, kvp.Value.Count));

                    statements.Add("ALTER TABLE " + kvp.Key + " DROP CONSTRAINT " + string.Join(", ", kvp.Value) + ";");
                }

                Context.Log(transactionId, LogSeverity.Debug, this, "{ForeignKeyCount} foreign keys acquired from information schema of {ConnectionStringName} in {Elapsed} for {TableCount} tables",
                    constraintsByTable.Sum(x => x.Value.Count), ConnectionString.Name, startedOn.Elapsed, _tableNamesAndCounts.Count);

                return statements;
            }
            catch (Exception ex)
            {
                var exception = new SqlSchemaReadException(this, "foreign key names", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "foreign key list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                    ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                exception.Data["ConnectionStringName"] = ConnectionString.Name;
                exception.Data["Statement"] = command.CommandText;
                exception.Data["Timeout"] = command.CommandTimeout;
                exception.Data["Elapsed"] = startedOn.Elapsed;

                ioCommand.Failed(exception);
                throw exception;
            }
        }
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var tableName = _tableNamesAndCounts[statementIndex].Item1;

        var ioCommand = Context.RegisterIoCommandStart(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(tableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "drop foreign keys",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "drop foreign keys", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop foreign keys, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["TableName"] = ConnectionString.Unescape(tableName);
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

        var fkCount = _tableNamesAndCounts
                .Take(lastSucceededIndex + 1)
                .Sum(x => x.Item2);

        Context.Log(transactionId, LogSeverity.Debug, this, "{ForeignKeyCount} foreign keys for {TableCount} table(s) successfully dropped on {ConnectionStringName}",
            fkCount, lastSucceededIndex + 1, ConnectionString.Name);
    }
}
