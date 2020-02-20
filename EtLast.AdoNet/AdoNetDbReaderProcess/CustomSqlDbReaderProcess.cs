namespace FizzCode.EtLast.AdoNet
{
    using System.Transactions;

    public class CustomSqlAdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string Sql { get; set; }

        public CustomSqlAdoNetDbReaderProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (string.IsNullOrEmpty(Sql))
                throw new ProcessParameterNullException(this, nameof(Sql));
        }

        protected override string CreateSqlStatement()
        {
            return Sql;
        }

        protected override void LogAction()
        {
            Context.Log(LogSeverity.Debug, this, "reading from {ConnectionStringName} using custom query, timeout: {Timeout} sec, transaction: {Transaction}",
                ConnectionString.Name, CommandTimeout, Transaction.Current.ToIdentifierString());
        }

        protected override void IncrementCounter()
        {
            CounterCollection.IncrementCounter("db records read", 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name + "/custom query", 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name + "/custom query/" + Name, 1);
        }
    }
}