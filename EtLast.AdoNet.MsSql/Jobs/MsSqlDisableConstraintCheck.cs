﻿namespace FizzCode.EtLast;

public sealed class MsSqlDisableConstraintCheck : AbstractSqlStatements
{
    [ProcessParameterMustHaveValue]
    public required string[] TableNames { get; init; }

    protected override List<string> CreateSqlStatements(INamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return TableNames.Select(tableName => "ALTER TABLE " + tableName + " NOCHECK CONSTRAINT ALL;").ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var tableName = TableNames[statementIndex];

        var ioCommand = Context.RegisterIoCommand(new IoCommand()
        {
            Process = this,
            Kind = IoCommandKind.dbAlterSchema,
            Location = ConnectionString.Name,
            Path = ConnectionString.Unescape(tableName),
            TimeoutSeconds = command.CommandTimeout,
            Command = command.CommandText,
            TransactionId = transactionId,
            Message = "disable constraint check",
        });

        try
        {
            command.ExecuteNonQuery();
            ioCommand.End();
        }
        catch (Exception ex)
        {
            var exception = new SqlSchemaChangeException(this, "disable constraint check", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to disable constraint check, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
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

        Context.Log(transactionId, LogSeverity.Debug, this, "constraint check successfully disabled on {TableCount} tables on {ConnectionStringName}",
            lastSucceededIndex + 1, ConnectionString.Name);
    }
}
