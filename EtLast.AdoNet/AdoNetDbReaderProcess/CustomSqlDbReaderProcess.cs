namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Transactions;

    public class CustomSqlAdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string Sql { get; set; }

        public CustomSqlAdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(ICaller caller = null)
        {
            Caller = caller;

            if (string.IsNullOrEmpty(Sql))
                throw new ProcessParameterNullException(this, nameof(Sql));

            return base.Evaluate(caller);
        }

        protected override string CreateSqlStatement()
        {
            return Sql;
        }

        protected override void LogAction()
        {
            Context.Log(LogSeverity.Debug, this, "reading from {ConnectionStringKey} using custom query, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");
        }

        protected override void IncrementCounter()
        {
            Context.Stat.IncrementCounter("database records read / " + ConnectionString.Name, 1);
            Context.Stat.IncrementDebugCounter("database records read / " + ConnectionString.Name + " / custom query", 1);
            Context.Stat.IncrementDebugCounter("database records read / " + ConnectionString.Name + " / custom query / " + Name, 1);
        }
    }
}