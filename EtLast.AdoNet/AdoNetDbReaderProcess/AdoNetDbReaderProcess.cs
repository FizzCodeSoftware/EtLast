namespace FizzCode.EtLast.AdoNet
{
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class AdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string TableName { get; set; }
        public string CustomWhereClause { get; set; }
        public string CustomOrderByClause { get; set; }
        public int RecordCountLimit { get; set; }

        public AdoNetDbReaderProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
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
                columnList = string.Join(", ", ColumnConfiguration.Select(x => x.SourceColumn));
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
                if (ConnectionString.KnownProvider == KnownProvider.MySql)
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
            Context.Log(LogSeverity.Debug, this, "reading from {ConnectionStringName}/{TableName}, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, ConnectionString.Unescape(TableName), CommandTimeout, Transaction.Current.ToIdentifierString());
        }

        protected override void IncrementCounter()
        {
            CounterCollection.IncrementCounter("db records read", 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name, 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableName), 1);
        }
    }
}