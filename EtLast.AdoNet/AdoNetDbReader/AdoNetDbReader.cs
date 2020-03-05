namespace FizzCode.EtLast.AdoNet
{
    using System.Globalization;
    using System.Linq;
    using FizzCode.DbTools.Configuration;

    public class AdoNetDbReader : AbstractAdoNetDbReader
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }
        public string CustomOrderByClause { get; set; }
        public int RecordCountLimit { get; set; }

        public AdoNetDbReader(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableName == null)
                throw new ProcessParameterNullException(this, nameof(TableName));
        }

        protected override string CreateSqlStatement()
        {
            var columnList = "*";
            if (ColumnConfiguration?.Count > 0)
            {
                columnList = string.Join(", ", ColumnConfiguration.Select(x => ConnectionString.Escape(x.SourceColumn)));
            }

            var prefix = "";
            var postfix = "";

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
                if (ConnectionString.SqlEngine == SqlEngine.MySql)
                {
                    postfix += (string.IsNullOrEmpty(postfix) ? "" : " ") + "LIMIT " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
                }
                else
                {
                    prefix = "TOP " + RecordCountLimit.ToString("D", CultureInfo.InvariantCulture);
                }
                // todo: support Oracle Syntax: https://www.w3schools.com/sql/sql_top.asp
            }

            return "SELECT "
                + (!string.IsNullOrEmpty(prefix) ? prefix + " " : "")
                + columnList
                + " FROM "
                + TableName
                + (!string.IsNullOrEmpty(postfix) ? " " + postfix : "");
        }

        protected override int RegisterIoCommandStart(string transactionId, int timeout, string statement)
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "querying from {ConnectionStringName}/{TableName}",
                ConnectionString.Name, ConnectionString.Unescape(TableName));
        }
    }
}