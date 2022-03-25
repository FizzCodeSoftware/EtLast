namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using FizzCode.LightWeight.AdoNet;

public sealed class MsSqlDropSchemas : AbstractSqlStatements
{
    public string[] SchemaNames { get; init; }

    public MsSqlDropSchemas(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        base.ValidateImpl();

        if (SchemaNames == null || SchemaNames.Length == 0)
            throw new ProcessParameterNullException(this, nameof(SchemaNames));

        if (ConnectionString.SqlEngine != SqlEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
    {
        return SchemaNames
            .Where(x => !string.Equals(x, "dbo", StringComparison.InvariantCultureIgnoreCase))
            .Select(x => "DROP SCHEMA IF EXISTS " + x + ";")
            .ToList();
    }

    protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
    {
        var schemaName = SchemaNames[statementIndex];
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(schemaName), command.CommandTimeout, command.CommandText, transactionId, null,
            "drop schema {ConnectionStringName}/{SchemaName}",
            ConnectionString.Name, ConnectionString.Unescape(schemaName));

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
        }
        catch (Exception ex)
        {
            Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

            var exception = new SqlSchemaChangeException(this, "drop schema", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop schema, connection string key: {0}, schema: {1}, message: {2}, command: {3}, timeout: {4}",
                ConnectionString.Name, ConnectionString.Unescape(schemaName), ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data.Add("ConnectionStringName", ConnectionString.Name);
            exception.Data.Add("SchemaName", ConnectionString.Unescape(schemaName));
            exception.Data.Add("Statement", command.CommandText);
            exception.Data.Add("Timeout", command.CommandTimeout);
            exception.Data.Add("Elapsed", startedOn.Elapsed);
            throw exception;
        }
    }

    protected override void LogSucceeded(int lastSucceededIndex, string transactionId)
    {
        if (lastSucceededIndex == -1)
            return;

        Context.Log(transactionId, LogSeverity.Debug, this, "{SchemaCount} schema(s) successfully dropped on {ConnectionStringName} in {Elapsed}",
            lastSucceededIndex + 1, ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
    }
}
