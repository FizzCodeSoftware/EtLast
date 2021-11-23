namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using FizzCode.LightWeight.AdoNet;

    public sealed class CopyTableStructure : AbstractSqlStatements
    {
        public List<TableCopyConfiguration> Configuration { get; init; }

        public CopyTableStructure(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (Configuration == null)
                throw new ProcessParameterNullException(this, nameof(Configuration));

            if (Configuration.Any(x => string.IsNullOrEmpty(x.SourceTableName)))
                throw new ProcessParameterNullException(this, nameof(TableCopyConfiguration.SourceTableName));

            if (Configuration.Any(x => string.IsNullOrEmpty(x.TargetTableName)))
                throw new ProcessParameterNullException(this, nameof(TableCopyConfiguration.TargetTableName));
        }

        protected override List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId)
        {
            var statements = new List<string>();
            var sb = new StringBuilder();

            foreach (var config in Configuration)
            {
                var columnList = (config.ColumnConfiguration == null || config.ColumnConfiguration.Count == 0)
                    ? "*"
                    : string.Join(", ", config.ColumnConfiguration.Select(x => x.FromColumn + (x.ToColumn != x.FromColumn ? " AS " + x.ToColumn : "")));

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
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbAlterSchema, ConnectionString.Name, ConnectionString.Unescape(config.TargetTableName), command.CommandTimeout, command.CommandText, transactionId, null,
                "create new table {ConnectionStringName}/{TargetTableName} based on {SourceTableName}",
                ConnectionString.Name, ConnectionString.Unescape(config.TargetTableName), ConnectionString.Unescape(config.SourceTableName));

            try
            {
                command.ExecuteNonQuery();
                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbAlterSchema, iocUid, null);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbAlterSchema, iocUid, null, ex);

                var exception = new SqlSchemaChangeException(this, "copy table structure", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to create table, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                    ConnectionString.Name, ConnectionString.Unescape(config.SourceTableName), ConnectionString.Unescape(config.TargetTableName), config.ColumnConfiguration != null ? string.Join(",", config.ColumnConfiguration.Select(x => x.FromColumn)) : "all", ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("SourceTableName", ConnectionString.Unescape(config.SourceTableName));
                exception.Data.Add("TargetTableName", ConnectionString.Unescape(config.TargetTableName));
                if (config.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", config.ColumnConfiguration.Select(x => ConnectionString.Unescape(x.FromColumn))));
                }

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

            Context.Log(transactionId, LogSeverity.Debug, this, "{TableCount} table(s) successfully created on {ConnectionStringName} in {Elapsed}", lastSucceededIndex + 1,
                ConnectionString.Name, InvocationInfo.LastInvocationStarted.Elapsed);
        }
    }
}