namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class CopyTableStructureJob : AbstractSqlStatementsJob
    {
        public List<TableCopyConfiguration> Configuration { get; set; }

        protected override void Validate()
        {
            if (Configuration == null)
                throw new JobParameterNullException(Process, this, nameof(Configuration));

            if (Configuration.Any(x => string.IsNullOrEmpty(x.SourceTableName)))
                throw new JobParameterNullException(Process, this, nameof(TableCopyConfiguration.SourceTableName));
            if (Configuration.Any(x => string.IsNullOrEmpty(x.TargetTableName)))
                throw new JobParameterNullException(Process, this, nameof(TableCopyConfiguration.TargetTableName));
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
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

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var config = Configuration[statementIndex];

            Process.Context.Log(LogSeverity.Debug, Process, this, null, "create new table {ConnectionStringKey}/{TargetTableName} based on {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, Helpers.UnEscapeTableName(config.TargetTableName), Helpers.UnEscapeTableName(config.SourceTableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                Process.Context.Log(LogSeverity.Debug, Process, this, null, "table {ConnectionStringKey}/{TargetTableName} is created from {SourceTableName} in {Elapsed}, transaction: {Transaction}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(config.TargetTableName), Helpers.UnEscapeTableName(config.SourceTableName), startedOn.Elapsed, Transaction.Current.ToIdentifierString());
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "failed to copy table structure", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to copy table structure, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(config.SourceTableName), Helpers.UnEscapeTableName(config.TargetTableName), config.ColumnConfiguration != null ? string.Join(",", config.ColumnConfiguration.Select(x => x.FromColumn)) : "all", ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("SourceTableName", Helpers.UnEscapeTableName(config.SourceTableName));
                exception.Data.Add("TargetTableName", Helpers.UnEscapeTableName(config.TargetTableName));
                if (config.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", config.ColumnConfiguration.Select(x => Helpers.UnEscapeColumnName(x.FromColumn))));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            Process.Context.Log(LogSeverity.Information, Process, this, null, "{TableCount} table(s) successfully created on {ConnectionStringKey} in {Elapsed}",
                lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed);
        }
    }
}