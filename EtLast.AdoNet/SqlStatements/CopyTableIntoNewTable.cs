namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    public class CopyTableIntoNewTable : AbstractSqlStatement
    {
        public TableCopyConfiguration Configuration { get; init; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the old table to the new table.
        /// </summary>
        public string WhereClause { get; init; }

        public CopyTableIntoNewTable(ITopic topic, string name)
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

            var dropTableStatement = (ConnectionString.SqlEngine, ConnectionString.Version) switch
            {
                (SqlEngine.MsSql, "2005" or "2008" or "2008 R2" or "2008R2" or "2012" or "2014")
                    => "IF OBJECT_ID('" + Configuration.TargetTableName + "', 'U') IS NOT NULL DROP TABLE " + Configuration.TargetTableName,
                _ => "DROP TABLE IF EXISTS " + Configuration.TargetTableName,
            };

            var statement = dropTableStatement + "; SELECT " + columnList + " INTO " + Configuration.TargetTableName + " FROM " + Configuration.SourceTableName;

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            return statement;
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteCopy, ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                "creating new table {ConnectionStringName}/{TargetTableName} and copying records from {SourceTableName}",
                ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), ConnectionString.Unescape(Configuration.SourceTableName));
            try
            {
                var recordCount = command.ExecuteNonQuery();

                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteCopy, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteCopy, iocUid, null, ex);

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