namespace FizzCode.EtLast.AdoNet
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
        public string[] Columns { get; set; }
        public int RecordCountLimit { get; set; } = 0;

        public AdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        protected virtual string GetTransformedTableName()
        {
            return TableName;
        }

        protected virtual string GetTransformedColumn(string column)
        {
            return column;
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(TableName))
                throw new ProcessParameterNullException(this, nameof(TableName));

            return base.Evaluate(caller);
        }

        protected override string CreateSqlStatement()
        {
            List<string> columns = null;
            if (Columns != null)
            {
                columns = new List<string>();
                foreach (var column in Columns)
                {
                    string trCol;
                    if (column.Contains("=>"))
                    {
                        var parts = column.Split(new[] { "=>" }, StringSplitOptions.RemoveEmptyEntries);
                        trCol = GetTransformedColumn(parts[0].Trim()) + " AS " + parts[1].Trim() + ""; // removed automatic [x] escaping because it works only for MsSql
                    }
                    else
                    {
                        trCol = GetTransformedColumn(column);
                    }

                    columns.Add(trCol);
                }
            }

            var tableName = GetTransformedTableName();

            var columnList = columns?.Count > 0
                ? string.Join(", ", columns)
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
                var isMySql = string.Compare(ConnectionStringSettings.ProviderName, "MySql.Data.MySqlClient", StringComparison.InvariantCultureIgnoreCase) == 0;
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

            return "SELECT " + (!string.IsNullOrEmpty(prefix) ? prefix + " " : "") + columnList + " FROM " + tableName + (!string.IsNullOrEmpty(postfix) ? " " + postfix : "");
        }

        protected override void LogAction()
        {
            Context.Log(LogSeverity.Information, this, "reading from {ConnectionStringKey}/{TableName}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, TableName, CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");
        }
    }
}