﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using FizzCode.DbTools.Configuration;

    public class CopyTableStructureProcess : AbstractSqlStatementsProcess
    {
        public List<TableCopyConfiguration> Configuration { get; set; }

        public CopyTableStructureProcess(ITopic topic, string name)
            : base(topic, name)
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

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection, string transactionId)
        {
            var statements = new List<string>();
            var sb = new StringBuilder();

            foreach (var config in Configuration)
            {
                var columnList = (config.ColumnConfiguration == null || config.ColumnConfiguration.Count == 0)
                    ? "*"
                    : string.Join(", ", config.ColumnConfiguration.Select(x => x.FromColumn + (x.ToColumn != x.FromColumn ? " AS " + x.ToColumn : "")));

                sb.Append("DROP TABLE IF EXISTS ")
                    .Append(config.TargetTableName)
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

        protected override void LogAction(int statementIndex, string transactionId)
        {
            var config = Configuration[statementIndex];

            Context.Log(transactionId, LogSeverity.Debug, this, "create new table {ConnectionStringName}/{TargetTableName} based on {SourceTableName}",
                ConnectionString.Name, ConnectionString.Unescape(config.TargetTableName), ConnectionString.Unescape(config.SourceTableName));
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId)
        {
            var config = Configuration[statementIndex];
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, null);
            try
            {
                command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, null);

                Context.Log(transactionId, LogSeverity.Debug, this, "table {ConnectionStringName}/{TargetTableName} is created from {SourceTableName} in {Elapsed}",
                    ConnectionString.Name, ConnectionString.Unescape(config.TargetTableName), ConnectionString.Unescape(config.SourceTableName), startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "failed to copy table structure", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to copy table structure, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
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