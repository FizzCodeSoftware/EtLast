﻿namespace FizzCode.EtLast;

public sealed class MsSqlResetSingleIdentityCounter : AbstractSqlStatement
{
    public string TableName { get; init; }
    public string IdentityColumnName { get; init; }

    public MsSqlResetSingleIdentityCounter(IEtlContext context)
        : base(context)
    {
    }

    public override string GetTopic()
    {
        return TableName != null
            ? ConnectionString?.Unescape(TableName)
            : null;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (TableName == null)
            throw new ProcessParameterNullException(this, nameof(TableName));

        if (IdentityColumnName == null)
            throw new ProcessParameterNullException(this, nameof(IdentityColumnName));

        if (ConnectionString.SqlEngine != SqlEngine.MsSql)
            throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be Microsoft.Data.SqlClient");
    }

    protected override string CreateSqlStatement(Dictionary<string, object> parameters)
    {
        return "declare @max int select @max=ISNULL(max(" + IdentityColumnName + "),0) from " + TableName + "; DBCC CHECKIDENT ('" + TableName + "', RESEED, @max);";
    }

    protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
    {
        var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbIdentityReset, ConnectionString.Name, TableName, command.CommandTimeout, command.CommandText, transactionId, () => parameters,
            "resetting identity counter on {ConnectionStringName}/{TableName}.{IdentityColumnName}",
            ConnectionString.Name, TableName, IdentityColumnName);

        try
        {
            command.ExecuteNonQuery();
            Context.RegisterIoCommandSuccess(this, IoCommandKind.dbIdentityReset, iocUid, null);
        }
        catch (Exception ex)
        {
            var exception = new SqlIdentityResetException(this, ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "identity counter reset failed, connection string key: {0}, message: {1}, command: {2}, timeout: {3}",
                ConnectionString.Name, ex.Message, command.CommandText, command.CommandTimeout));

            exception.Data["ConnectionStringName"] = ConnectionString.Name;
            exception.Data["Table"] = TableName;
            exception.Data["IdentityColumn"] = IdentityColumnName;
            exception.Data["Statement"] = command.CommandText;
            exception.Data["Timeout"] = command.CommandTimeout;
            exception.Data["Elapsed"] = InvocationInfo.LastInvocationStarted.Elapsed;

            Context.RegisterIoCommandFailed(this, IoCommandKind.dbIdentityReset, iocUid, null, exception);
            throw exception;
        }
    }
}
