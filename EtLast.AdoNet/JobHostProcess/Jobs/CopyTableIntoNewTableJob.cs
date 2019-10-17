﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class CopyTableIntoNewTableJob : AbstractSqlStatementJob
    {
        public TableCopyConfiguration Configuration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
        /// </summary>
        public string WhereClause { get; set; }

        protected override void Validate()
        {
            if (Configuration == null)
                throw new JobParameterNullException(Process, this, nameof(Configuration));
            if (string.IsNullOrEmpty(Configuration.SourceTableName))
                throw new JobParameterNullException(Process, this, nameof(Configuration.SourceTableName));
            if (string.IsNullOrEmpty(Configuration.TargetTableName))
                throw new JobParameterNullException(Process, this, nameof(Configuration.TargetTableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString)
        {
            var columnList = (Configuration.ColumnConfiguration == null || Configuration.ColumnConfiguration.Count == 0)
                 ? "*"
                 : string.Join(", ", Configuration.ColumnConfiguration.Select(x => x.FromColumn + " AS " + x.ToColumn));

            var statement = "DROP TABLE IF EXISTS " + Configuration.TargetTableName + "; SELECT " + columnList + " INTO " + Configuration.TargetTableName + " FROM " + Configuration.SourceTableName;

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            return statement;
        }

        protected override void RunCommand(IDbCommand command, Stopwatch startedOn)
        {
            Process.Context.Log(LogSeverity.Debug, Process, "({Job}) creating new table {ConnectionStringKey}/{TargetTableName} and copying records from {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(Configuration.TargetTableName), Helpers.UnEscapeTableName(Configuration.SourceTableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var recordCount = command.ExecuteNonQuery();

                Process.Context.Log(LogSeverity.Information, Process, "({Job}) table {ConnectionStringKey}/{TargetTableName} created and {RecordCount} records copied from {SourceTableName} in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(Configuration.TargetTableName), recordCount, Helpers.UnEscapeTableName(Configuration.SourceTableName), startedOn.Elapsed);

                // todo: support stats in jobs...
                // Stat.IncrementCounter("records copied", recordCount);
                // Stat.IncrementCounter("copy time", startedOn.ElapsedMilliseconds);

                Process.Context.Stat.IncrementCounter("database records copied / " + ConnectionString.Name, recordCount);
                Process.Context.Stat.IncrementDebugCounter("database records copied / " + ConnectionString.Name + " / " + Helpers.UnEscapeTableName(Configuration.SourceTableName) + " -> " + Helpers.UnEscapeTableName(Configuration.TargetTableName), recordCount);
                Process.Context.Stat.IncrementCounter("database copy time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
                Process.Context.Stat.IncrementDebugCounter("database copy time / " + ConnectionString.Name + " / " + Helpers.UnEscapeTableName(Configuration.SourceTableName) + " -> " + Helpers.UnEscapeTableName(Configuration.TargetTableName), startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(Process, this, "database table creation and copy failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table creation and copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(Configuration.SourceTableName), Helpers.UnEscapeTableName(Configuration.TargetTableName),
                    Configuration.ColumnConfiguration != null
                        ? string.Join(",", Configuration.ColumnConfiguration.Select(x => x.FromColumn))
                        : "all",
                    ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("SourceTableName", Helpers.UnEscapeTableName(Configuration.SourceTableName));
                exception.Data.Add("TargetTableName", Helpers.UnEscapeTableName(Configuration.TargetTableName));
                if (Configuration.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", Configuration.ColumnConfiguration.Select(x => Helpers.UnEscapeColumnName(x.FromColumn))));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}