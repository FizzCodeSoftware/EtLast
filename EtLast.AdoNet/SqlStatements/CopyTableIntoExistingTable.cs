namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using FizzCode.LightWeight.AdoNet;

    public sealed class CopyTableIntoExistingTable : AbstractSqlStatement
    {
        public TableCopyConfiguration Configuration { get; init; }

        /// <summary>
        /// Optional. Default is NULL which means everything will be transferred from the source table to the target table.
        /// </summary>
        public string WhereClause { get; init; }

        public bool CopyIdentityColumns { get; init; }

        public Dictionary<string, object> ColumnDefaults { get; init; }

        public CopyTableIntoExistingTable(IEtlContext context)
            : base(context)
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
            var statement = "";
            if (CopyIdentityColumns && ConnectionString.SqlEngine == SqlEngine.MsSql)
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
                        var paramName = "_" + ConnectionString.Unescape(kvp.Key);
                        sourceColumnList += ", @" + paramName + " as " + kvp.Key;
                        targetColumnList += ", " + kvp.Key;
                        parameters.Add(paramName, kvp.Value ?? DBNull.Value);
                        index++;
                    }
                }

                statement += "INSERT INTO " + Configuration.TargetTableName + " (" + targetColumnList + ") SELECT " + sourceColumnList + " FROM " + Configuration.SourceTableName;
            }

            if (WhereClause != null)
            {
                statement += " WHERE " + WhereClause.Trim();
            }

            if (CopyIdentityColumns && ConnectionString.SqlEngine == SqlEngine.MsSql)
            {
                statement += "; SET IDENTITY_INSERT " + Configuration.TargetTableName + " OFF; ";
            }

            return statement;
        }

        protected override void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters)
        {
            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteCopy, ConnectionString.Name, ConnectionString.Unescape(Configuration.TargetTableName), command.CommandTimeout, command.CommandText, transactionId, () => parameters,
                "copying records from {ConnectionStringName}/{SourceTableName} to {TargetTableName}",
                ConnectionString.Name, ConnectionString.Unescape(Configuration.SourceTableName), ConnectionString.Unescape(Configuration.TargetTableName));

            try
            {
                var recordCount = command.ExecuteNonQuery();

                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteCopy, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteCopy, iocUid, null, ex);

                var exception = new SqlSchemaChangeException(this, "copy table into existing", ex);
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
                exception.Data.Add("Elapsed", InvocationInfo.LastInvocationStarted.Elapsed);
                throw exception;
            }
        }
    }
}