﻿namespace FizzCode.EtLast;

public enum MsSqlDropStoredProceduresProcessMode { All, SpecifiedStoredProcedures, InSpecifiedSchema }

public sealed class MsSqlDropStoredProcedures : AbstractSqlStatements
{
    /// <summary>
    /// Default value is <see cref="MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures"/>
    /// </summary>
    public required MsSqlDropStoredProceduresProcessMode Mode { get; init; } = MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures;

    /// <summary>
    /// Must be set if <see cref="Mode"/> is set to <see cref="MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema"/>
    /// </summary>
    public string SchemaName { get; init; }

    /// <summary>
    /// Stored procedure names must include schema name.
    /// </summary>
    public string[] StoredProcedureNames { get; init; }

    private List<string> _storedProcedureNames;

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        switch (Mode)
        {
            case MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures:
                if (StoredProcedureNames == null || StoredProcedureNames.Length == 0)
                    throw new ProcessParameterNullException(this, nameof(StoredProcedureNames));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures));
                break;
            case MsSqlDropStoredProceduresProcessMode.All:
                if (StoredProcedureNames != null)
                    throw new InvalidProcessParameterException(this, nameof(StoredProcedureNames), StoredProcedureNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
                if (!string.IsNullOrEmpty(SchemaName))
                    throw new InvalidProcessParameterException(this, nameof(SchemaName), SchemaName, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
                break;
            case MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema:
                if (StoredProcedureNames != null)
                    throw new InvalidProcessParameterException(this, nameof(StoredProcedureNames), StoredProcedureNames, "Value must be null if " + nameof(Mode) + " is set to " + nameof(MsSqlDropStoredProceduresProcessMode.All));
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
            case MsSqlDropStoredProceduresProcessMode.SpecifiedStoredProcedures:
                _storedProcedureNames = [.. StoredProcedureNames];
                break;

            case MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema:
            case MsSqlDropStoredProceduresProcessMode.All:
                var startedOn = Stopwatch.StartNew();
                using (var command = connection.CreateCommand())
                {
                    var parameters = new Dictionary<string, object>();

                    command.CommandTimeout = CommandTimeout;
                    command.CommandText = "select * from INFORMATION_SCHEMA.ROUTINES where ROUTINE_TYPE = 'PROCEDURE'";
                    if (Mode == MsSqlDropStoredProceduresProcessMode.InSpecifiedSchema)
                    {
                        command.CommandText += " AND ROUTINE_SCHEMA = @schemaName";
                        parameters.Add("schemaName", SchemaName);
                    }

                    foreach (var kvp in parameters)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = kvp.Key;
                        parameter.Value = kvp.Value;
                        command.Parameters.Add(parameter);
                    }

                    _storedProcedureNames = [];

                    var ioCommand = Context.RegisterIoCommand(new IoCommand()
                    {
                        Process = this,
                        Kind = IoCommandKind.dbReadMeta,
                        Location = ConnectionString.Name,
                        Path = "INFORMATION_SCHEMA.ROUTINES",
                        TimeoutSeconds = command.CommandTimeout,
                        Command = command.CommandText,
                        TransactionId = transactionId,
                        Message = "querying stored procedures names",
                    });

                    try
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                _storedProcedureNames.Add(ConnectionString.Escape((string)reader["ROUTINE_NAME"], (string)reader["ROUTINE_SCHEMA"]));
                            }

                            ioCommand.AffectedDataCount += _storedProcedureNames.Count;
                            ioCommand.End();
                        }

                        _storedProcedureNames.Sort();
                    }
                    catch (Exception ex)
                    {
                        var exception = new SqlSchemaReadException(this, "stored procedure name names", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "stored procedure list query failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
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

        return _storedProcedureNames
            .ConvertAll(storedProcedureName => "DROP PROCEDURE IF EXISTS " + storedProcedureName + ";")
;
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var storedProcedureName = _storedProcedureNames[statementIndex];

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(storedProcedureName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "drop strored procedure",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "drop stored procedure", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop stored procedure, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(storedProcedureName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["StoredProcedureName"] = ConnectionString.Unescape(storedProcedureName);
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

        Context.Log(transactionId, LogSeverity.Debug, this, "{StoredProcedureCount} stored procedure(s) successfully dropped on {ConnectionStringName}", lastSucceededIndex + 1,
            ConnectionString.Name);
    }
}
