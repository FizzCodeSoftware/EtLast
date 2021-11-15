namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    public sealed class DropTables : AbstractSqlStatements
    {
        public string[] TableNames { get; init; }

        public DropTables(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableNames == null || TableNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(TableNames));
        }

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
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbReadCount, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
                "querying record count from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(tableName));
            try
            {
                recordCount = (int)command.ExecuteScalar();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, iocUid, recordCount);
            }
            catch (Exception)
            {
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbReadCount, iocUid, null);
            }

            command.CommandText = originalStatement;
            iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(tableName), command.CommandTimeout, command.CommandText, transactionId, null,
            "drop table {ConnectionStringName}/{TableName}",
            ConnectionString.Name, ConnectionString.Unescape(tableName));

            try
            {
                command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

                var exception = new SqlSchemaChangeException(this, "drop table", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(tableName));
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

            Context.Log(transactionId, LogSeverity.Debug, this, "{TableCount} table(s) successfully dropped on {ConnectionStringName} in {Elapsed}", lastSucceededIndex + 1,
                ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}