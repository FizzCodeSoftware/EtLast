namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Transactions;

    public class CopyTableStructureJob : AbstractSqlStatementsJob
    {
        public List<TableCopyConfiguration> Configuration { get; set; }

        protected override void Validate(IProcess process)
        {
            if (Configuration == null)
                throw new JobParameterNullException(process, this, nameof(Configuration));

            if (Configuration.Any(x => string.IsNullOrEmpty(x.SourceTableName)))
                throw new JobParameterNullException(process, this, nameof(TableCopyConfiguration.SourceTableName));
            if (Configuration.Any(x => string.IsNullOrEmpty(x.TargetTableName)))
                throw new JobParameterNullException(process, this, nameof(TableCopyConfiguration.TargetTableName));
        }

        protected override List<string> CreateSqlStatements(IProcess process, ConnectionStringSettings settings)
        {
            var statements = new List<string>();
            var sb = new StringBuilder();

            foreach (var config in Configuration)
            {
                var columnList = (config.ColumnConfiguration == null || config.ColumnConfiguration.Count == 0)
                    ? "*"
                    : string.Join(", ", config.ColumnConfiguration.Select(x => x.FromColumn + " AS " + x.ToColumn));

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

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var config = Configuration[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "create new table {ConnectionStringKey}/{TargetTableName} based on {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, Helpers.UnEscapeTableName(config.TargetTableName), Helpers.UnEscapeTableName(config.SourceTableName), command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                command.ExecuteNonQuery();

                process.Context.Log(LogSeverity.Debug, process, "table {ConnectionStringKey}/{TargetTableName} is created from {SourceTableName} in {Elapsed}",
                    ConnectionStringSettings.Name, Helpers.UnEscapeTableName(config.TargetTableName), Helpers.UnEscapeTableName(config.SourceTableName), startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to copy table structure", ex);
                exception.AddOpsMessage(string.Format("failed to copy table structure, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message {4}, command: {5}, timeout: {6}",
                    ConnectionStringSettings.Name, Helpers.UnEscapeTableName(config.SourceTableName), Helpers.UnEscapeTableName(config.TargetTableName), config.ColumnConfiguration != null ? string.Join(",", config.ColumnConfiguration.Select(x => x.FromColumn)) : "all", ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("SourceTableName", config.SourceTableName);
                exception.Data.Add("TargetTableName", config.TargetTableName);
                if (config.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", config.ColumnConfiguration.Select(x => x.FromColumn)));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(IProcess process, int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            process.Context.Log(LogSeverity.Information, process, "table(s) successfully created on {ConnectionStringKey} in {Elapsed}: {TableNames}",
                ConnectionStringSettings.Name, startedOn.Elapsed,
                Configuration
                    .Take(lastSucceededIndex + 1)
                    .Select(config => Helpers.UnEscapeTableName(config.SourceTableName) + "->" + Helpers.UnEscapeTableName(config.TargetTableName))
                    .ToArray());
        }
    }
}