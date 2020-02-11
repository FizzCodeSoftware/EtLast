namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class CopyTableIntoExistingTableProcess : AbstractSqlStatementProcess
    {
        public TableCopyConfiguration Configuration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the source table to the target table.
        /// </summary>
        public string WhereClause { get; set; }

        public bool CopyIdentityColumns { get; set; }

        public Dictionary<string, object> ColumnDefaults { get; set; }

        public CopyTableIntoExistingTableProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (Configuration == null)
                throw new ProcessParameterNullException(this, nameof(Configuration));

            if (string.IsNullOrEmpty(Configuration.SourceTableName))
                throw new ProcessParameterNullException(this, nameof(Configuration.SourceTableName));

            if (string.IsNullOrEmpty(Configuration.TargetTableName))
                throw new ProcessParameterNullException(this, nameof(Configuration.TargetTableName));
        }

        protected override string CreateSqlStatement(ConnectionStringWithProvider connectionString, Dictionary<string, object> parameters)
        {
            var statement = "";
            if (CopyIdentityColumns && ConnectionString.KnownProvider == KnownProvider.SqlServer)
            {
                if (Configuration.ColumnConfiguration == null || Configuration.ColumnConfiguration.Count == 0)
                    throw new InvalidProcessParameterException(this, nameof(Configuration) + "." + nameof(TableCopyConfiguration.ColumnConfiguration), null, "identity columns can be copied only if the column list is specified");

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

                if (ColumnDefaults != null)
                {
                    var index = 0;
                    foreach (var kvp in ColumnDefaults)
                    {
                        var parameName = "@colDef" + index.ToString("D", CultureInfo.InvariantCulture);
                        sourceColumnList += ", " + parameName + " as " + kvp.Key;
                        targetColumnList += ", " + kvp.Key;
                        parameters.Add(parameName, kvp.Value ?? DBNull.Value);
                        index++;
                    }
                }

                statement += "INSERT INTO " + Configuration.TargetTableName + " (" + targetColumnList + ") SELECT " + sourceColumnList + " FROM " + Configuration.SourceTableName;
            }

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            if (CopyIdentityColumns && ConnectionString.KnownProvider == KnownProvider.SqlServer)
            {
                statement += "; SET IDENTITY_INSERT " + Configuration.TargetTableName + " OFF; ";
            }

            return statement;
        }

        protected override void RunCommand(IDbCommand command)
        {
            Context.LogNoDiag(LogSeverity.Debug, this, "copying records from {ConnectionStringName}/{SourceTableName} to {TargetTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var recordCount = command.ExecuteNonQuery();

                var time = LastInvocationStarted.Elapsed;

                Context.Log(LogSeverity.Debug, this, "{RecordCount} records copied to {ConnectionStringName}/{TargetTableName} from {SourceTableName} in {Elapsed}, transaction: {Transaction}", recordCount,
                    ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), ConnectionString.Unescape(Configuration.SourceTableName), time, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db record copy count", recordCount);
                CounterCollection.IncrementTimeSpan("db record copy time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementCounter("db record copy count - " + ConnectionString.Name, recordCount);
                Context.CounterCollection.IncrementCounter("db record copy count - " + ConnectionString.Name + "/" + ConnectionString.Unescape(Configuration.SourceTableName) + " -> " + ConnectionString.Unescape(Configuration.TargetTableName), recordCount);
                Context.CounterCollection.IncrementTimeSpan("db record copy time - " + ConnectionString.Name, time);
                Context.CounterCollection.IncrementTimeSpan("db record copy time - " + ConnectionString.Name + "/" + ConnectionString.Unescape(Configuration.SourceTableName) + " -> " + ConnectionString.Unescape(Configuration.TargetTableName), time);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "database table copy failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                    ConnectionString.Name, ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName),
                    Configuration.ColumnConfiguration != null
                        ? string.Join(",", Configuration.ColumnConfiguration.Select(x => x.FromColumn))
                        : "all",
                    ex.Message, command.CommandText, CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("SourceTableName", ConnectionString.Unescape(Configuration.SourceTableName));
                exception.Data.Add("TargetTableName", ConnectionString.Unescape(Configuration.TargetTableName));
                if (Configuration.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", Configuration.ColumnConfiguration.Select(x => ConnectionString.Unescape(x.FromColumn))));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}