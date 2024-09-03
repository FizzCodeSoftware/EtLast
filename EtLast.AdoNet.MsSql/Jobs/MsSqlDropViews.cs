namespace FizzCode.EtLast;

public enum MsSqlDropViewsProcessMode { All, SpecifiedViews, SpecifiedSchema }

public sealed class MsSqlDropViews : AbstractSqlStatements
{
    /// <summary>
    /// Default value is <see cref="MsSqlDropViewsProcessMode.SpecifiedViews"/>
    /// </summary>
    public MsSqlDropViewsProcessMode Mode { get; init; } = MsSqlDropViewsProcessMode.SpecifiedViews;

    public string SchemaName { get; init; }
    public string[] ViewNames { get; init; }

    private List<string> _viewNames;

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        switch (Mode)
        {
            case MsSqlDropViewsProcessMode.SpecifiedViews:
                if (ViewNames == null || ViewNames.Length == 0)
                    throw new ProcessParameterNullException(this, nameof(ViewNames));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.SpecifiedViews));
                break;
            case MsSqlDropViewsProcessMode.All:
                if (ViewNames != null)
                    throw new InvalidProcessParameterException(this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                break;
            case MsSqlDropViewsProcessMode.SpecifiedSchema:
                if (ViewNames != null)
                    throw new InvalidProcessParameterException(this, nameof(ViewNames), ViewNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropViewsProcessMode.All));
                if (string.IsNullOrEmpty(SchemaName))
                    throw new ProcessParameterNullException(this, nameof(SchemaName));
                break;
        }

        if (ConnectionString.SqlEngine != AdoNetEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override List<string> CreateSqlStatements(INamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        switch (Mode)
        {
            case MsSqlDropViewsProcessMode.SpecifiedViews:
                _viewNames = [.. ViewNames];
                break;

            case MsSqlDropViewsProcessMode.SpecifiedSchema:
            case MsSqlDropViewsProcessMode.All:
                var startedOn = Stopwatch.StartNew();
                using (var command = connection.CreateCommand())
                {
                    var parameters = new Dictionary<string, object>();

                    command.CommandTimeout = CommandTimeout;
                    command.CommandText = "select * from INFORMATION_SCHEMA.VIEWS";
                    if (Mode == MsSqlDropViewsProcessMode.SpecifiedSchema)
                    {
                        command.CommandText += " where TABLE_SCHEMA = @schemaName";
                        parameters.Add("schemaName", SchemaName);
                    }

                    foreach (var kvp in parameters)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = kvp.Key;
                        parameter.Value = kvp.Value;
                        command.Parameters.Add(parameter);
                    }

                    _viewNames = [];

                    var ioCommand = Context.RegisterIoCommand(new IoCommand()
                    {
                        Process = this,
                        Kind = IoCommandKind.dbReadMeta,
                        Location = ConnectionString.Name,
                        Path = "INFORMATION_SCHEMA.VIEWS",
                        TimeoutSeconds = command.CommandTimeout,
                        Command = command.CommandText,
                        TransactionId = transactionId,
                        Message = "querying view names",
                    });

                    try
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                _viewNames.Add(ConnectionString.Escape((string)reader["TABLE_NAME"], (string)reader["TABLE_SCHEMA"]));
                            }

                            ioCommand.AffectedDataCount += _viewNames.Count;
                            ioCommand.End();
                        }

                        _viewNames.Sort();
                    }
                    catch (Exception ex)
                    {
                        var exception = new SqlSchemaReadException(this, "view names", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "view list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                            ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));
                        exception.Data["ConnectionStringName"] = ConnectionString.Name;
                        exception.Data["Statement"] = command.CommandText;
                        exception.Data["Timeout"] = command.CommandTimeout;
                        exception.Data["Elapsed"] = startedOn.Elapsed;

                        ioCommand.Failed(exception);
                        throw exception;
                    }
                }
                break;
        }

        return _viewNames
            .ConvertAll(viewName => "DROP VIEW IF EXISTS " + viewName + ";")
;
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var viewName = _viewNames[statementIndex];

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

        Context.Log(transactionId, LogSeverity.Debug, this, "{ViewCount} view(s) successfully dropped on {ConnectionStringName}", lastSucceededIndex + 1,
            ConnectionString.Name);
    }
}
