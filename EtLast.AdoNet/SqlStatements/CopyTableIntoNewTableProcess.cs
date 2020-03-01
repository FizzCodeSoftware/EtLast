namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;

    public class CopyTableIntoNewTableProcess : AbstractSqlStatementProcess
    {
        public TableCopyConfiguration Configuration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
        /// </summary>
        public string WhereClause { get; set; }

        public CopyTableIntoNewTableProcess(ITopic topic, string name)
            : base(topic, name)
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

        protected override string CreateSqlStatement(Dictionary<string, object> parameters)
        {
            var columnList = (Configuration.ColumnConfiguration == null || Configuration.ColumnConfiguration.Count == 0)
                 ? "*"
                 : string.Join(", ", Configuration.ColumnConfiguration.Select(x => x.FromColumn + (x.ToColumn != x.FromColumn ? " AS " + x.ToColumn : "")));

            var statement = "DROP TABLE IF EXISTS " + Configuration.TargetTableName + "; SELECT " + columnList + " INTO " + Configuration.TargetTableName + " FROM " + Configuration.SourceTableName;

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            return statement;
        }

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "creating new table {ConnectionStringName}/{TargetTableName} and copying records from {SourceTableName}",
                ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), ConnectionString.Unescape(Configuration.SourceTableName));
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var dscUid = Context.RegisterDataStoreCommandStart(this, DataStoreCommandKind.one, ConnectionString.Name, command.CommandTimeout, command.CommandText, transactionId, () => parameters);
            try
            {
                var recordCount = command.ExecuteNonQuery();
                Context.RegisterDataStoreCommandEnd(this, dscUid, recordCount, null);

                var time = InvocationInfo.LastInvocationStarted.Elapsed;

                Context.Log(transactionId, LogSeverity.Information, this, "table {ConnectionStringName}/{TargetTableName} created and {RecordCount} records copied from {SourceTableName} in {Elapsed}",
                    ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), recordCount, ConnectionString.Unescape(Configuration.SourceTableName), time);

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
                Context.RegisterDataStoreCommandEnd(this, dscUid, 0, ex.Message);

                var exception = new ProcessExecutionException(this, "database table creation and copy failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database table creation and copy failed, connection string key: {0}, source table: {1}, target table: {2}, source columns: {3}, message: {4}, command: {5}, timeout: {6}",
                    ConnectionString.Name, ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName),
                    Configuration.ColumnConfiguration != null
                        ? string.Join(",", Configuration.ColumnConfiguration.Select(x => x.FromColumn))
                        : "all",
                    ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("SourceTableName", ConnectionString.Unescape(Configuration.SourceTableName));
                exception.Data.Add("TargetTableName", ConnectionString.Unescape(Configuration.TargetTableName));
                if (Configuration.ColumnConfiguration != null)
                {
                    exception.Data.Add("SourceColumns", string.Join(",", Configuration.ColumnConfiguration.Select(x => ConnectionString.Unescape(x.FromColumn))));
                }

                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}