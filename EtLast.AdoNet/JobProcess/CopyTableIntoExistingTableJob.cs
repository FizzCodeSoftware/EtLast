namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    public class CopyTableIntoExistingTableJob : AbstractSqlStatementJob
    {
        public TableCopyConfiguration Configuration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the source table to the target table.
        /// </summary>
        public string WhereClause { get; set; }

        public bool CopyIdentityColumns { get; set; }

        protected override void Validate(IProcess process)
        {
            if (Configuration == null)
                throw new JobParameterNullException(process, this, nameof(Configuration));
            if (string.IsNullOrEmpty(Configuration.SourceTableName))
                throw new JobParameterNullException(process, this, nameof(Configuration.SourceTableName));
            if (string.IsNullOrEmpty(Configuration.TargetTableName))
                throw new JobParameterNullException(process, this, nameof(Configuration.TargetTableName));
        }

        protected override string CreateSqlStatement(IProcess process, ConnectionStringSettings settings)
        {
            var statement = string.Empty;
            if (CopyIdentityColumns && settings.ProviderName == "System.Data.SqlClient")
            {
                statement = "SET IDENTITY_INSERT " + Configuration.TargetTableName + " ON; ";
            }

            if (Configuration.ColumnConfiguration == null || Configuration.ColumnConfiguration.Count == 0)
            {
                statement += "INSERT INTO " + Configuration.TargetTableName + " SELECT * FROM " + Configuration.SourceTableName;
            }
            else
            {
                var sourceColumnList = string.Join(", ", Configuration.ColumnConfiguration.Select(x => x.FromColumn));
                var targetColumnList = string.Join(", ", Configuration.ColumnConfiguration.Select(x => x.ToColumn));

                statement += "INSERT INTO " + Configuration.TargetTableName + " (" + targetColumnList + ") SELECT " + sourceColumnList + " FROM " + Configuration.SourceTableName;
            }

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            if (CopyIdentityColumns && settings.ProviderName == "System.Data.SqlClient")
            {
                statement += "; SET IDENTITY_INSERT " + Configuration.TargetTableName + " OFF; ";
            }

            return statement;
        }

        protected override void RunCommand(IProcess process, IDbCommand command, Stopwatch startedOn)
        {
            process.Context.Log(LogSeverity.Debug, process, "copying records from {ConnectionStringKey}/{SourceTableName} to {TargetTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionStringSettings.Name, Helpers.UnEscapeTableName(Configuration.SourceTableName), Helpers.UnEscapeTableName(Configuration.TargetTableName), command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

            try
            {
                var recordCount = command.ExecuteNonQuery();

                process.Context.Log(LogSeverity.Information, process, "{RecordCount} records copied to {ConnectionStringKey}/{TargetTableName} from {SourceTableName} in {Elapsed}",
                    recordCount, ConnectionStringSettings.Name, Helpers.UnEscapeTableName(Configuration.TargetTableName), Helpers.UnEscapeTableName(Configuration.SourceTableName), startedOn.Elapsed);

                // todo: support stats in jobs...
                // Stat.IncrementCounter("records written", recordCount);
                // Stat.IncrementCounter("write time", startedOn.ElapsedMilliseconds);

                process.Context.Stat.IncrementCounter("database records copied / " + ConnectionStringSettings.Name, recordCount);
                process.Context.Stat.IncrementDebugCounter("database records copied / " + ConnectionStringSettings.Name + " / " + Helpers.UnEscapeTableName(Configuration.SourceTableName) + " -> " + Helpers.UnEscapeTableName(Configuration.TargetTableName), recordCount);
                process.Context.Stat.IncrementCounter("database copy time / " + ConnectionStringSettings.Name, startedOn.ElapsedMilliseconds);
                process.Context.Stat.IncrementDebugCounter("database copy time / " + ConnectionStringSettings.Name + " / " + Helpers.UnEscapeTableName(Configuration.SourceTableName) + " -> " + Helpers.UnEscapeTableName(Configuration.TargetTableName), startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "database table copy failed", ex);
                exception.AddOpsMessage(string.Format("database table copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message {4}, command: {5}, timeout: {6}",
                    ConnectionStringSettings.Name, Helpers.UnEscapeTableName(Configuration.SourceTableName), Helpers.UnEscapeTableName(Configuration.TargetTableName),
                    Configuration.ColumnConfiguration != null
                        ? string.Join(",", Configuration.ColumnConfiguration.Select(x => x.FromColumn))
                        : "all",
                    ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionStringSettings.Name);
                exception.Data.Add("SourceTableName", Configuration.SourceTableName);
                exception.Data.Add("TargetTableName", Configuration.TargetTableName);
                if (Configuration.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", Configuration.ColumnConfiguration.Select(x => x.FromColumn)));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }
    }
}