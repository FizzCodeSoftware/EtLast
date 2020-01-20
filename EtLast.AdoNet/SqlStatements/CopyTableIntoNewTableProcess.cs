namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class CopyTableIntoNewTableProcess : AbstractSqlStatementProcess
    {
        public TableCopyConfiguration Configuration { get; set; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
        /// </summary>
        public string WhereClause { get; set; }

        public CopyTableIntoNewTableProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
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

        protected override void RunCommand(IDbCommand command)
        {
            Context.Log(LogSeverity.Debug, this, "creating new table {ConnectionStringName}/{TargetTableName} and copying records from {SourceTableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(Configuration.TargetTableName), ConnectionString.Unescape(Configuration.SourceTableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                var recordCount = command.ExecuteNonQuery();

                var time = LastInvocation.Elapsed;

                Context.Log(LogSeverity.Information, this, "table {ConnectionStringName}/{TargetTableName} created and {RecordCount} records copied from {SourceTableName} in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(Configuration.TargetTableName), recordCount, ConnectionString.Unescape(Configuration.SourceTableName), time, Transaction.Current.ToIdentifierString());

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
                exception.Data.Add("Elapsed", LastInvocation.Elapsed);
                throw exception;
            }
        }
    }
}