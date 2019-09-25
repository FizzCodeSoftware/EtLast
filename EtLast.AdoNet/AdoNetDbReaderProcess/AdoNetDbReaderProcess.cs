﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Transactions;

    public class AdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string TableName { get; set; }

        public string CustomWhereClause { get; set; }
        public string CustomOrderByClause { get; set; }
        public int RecordCountLimit { get; set; }

        public AdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(ICaller caller = null)
        {
            Caller = caller;

            if (TableName == null)
                throw new ProcessParameterNullException(this, nameof(TableName));

            return base.Evaluate(caller);
        }

        protected override string CreateSqlStatement()
        {
            List<string> dbColumns = null;
            if (ColumnConfiguration != null)
            {
                dbColumns = new List<string>();
                foreach (var column in ColumnConfiguration)
                {
                    dbColumns.Add(column.SourceColumn);
                }
            }

            var columnList = dbColumns?.Count > 0
                ? string.Join(", ", dbColumns)
                : "*";

            var prefix = string.Empty;
            var postfix = string.Empty;

            if (!string.IsNullOrEmpty(CustomWhereClause))
            {
                postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "WHERE " + CustomWhereClause;
            }

            if (!string.IsNullOrEmpty(CustomOrderByClause))
            {
                postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "ORDER BY " + CustomOrderByClause;
            }

            if (RecordCountLimit > 0)
            {
                var isMySql = string.Equals(ConnectionString.ProviderName, "MySql.Data.MySqlClient", StringComparison.InvariantCultureIgnoreCase);
                if (isMySql)
                {
                    postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "LIMIT " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
                }
                else
                {
                    prefix = "TOP " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
                }
                // todo: support Oracle Syntax: https://www.w3schools.com/sql/sql_top.asp
            }

            return "SELECT " + (!string.IsNullOrEmpty(prefix) ? prefix + " " : "") + columnList + " FROM " + TableName + (!string.IsNullOrEmpty(postfix) ? " " + postfix : "");
        }

        protected override void LogAction()
        {
            Context.Log(LogSeverity.Debug, this, "reading from {ConnectionStringKey}/{TableName}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, Helpers.UnEscapeTableName(TableName), CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");
        }

        protected override void IncrementCounter()
        {
            Context.Stat.IncrementCounter("database records read / " + ConnectionString.Name, 1);
            Context.Stat.IncrementDebugCounter("database records read / " + ConnectionString.Name + " / " + Helpers.UnEscapeTableName(TableName), 1);
        }
    }
}