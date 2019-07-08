namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Transactions;

    public class CustomSqlAdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string Sql { get; set; }

        public CustomSqlAdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
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
            Context.Log(LogSeverity.Information, this, "reading from {ConnectionStringKey} using custom query, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");
        }

        protected override void IncrementCounter()
        {
            Context.Stat.IncrementCounter("database records read / " + ConnectionStringKey, 1);
            Context.Stat.IncrementCounter("database records read / " + ConnectionStringKey + " / custom query / " + Name, 1);
        }
    }
}