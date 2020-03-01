namespace FizzCode.EtLast.AdoNet
{
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

        protected override void LogAction(string transactionId)
        {
            Context.Log(transactionId, LogSeverity.Debug, this, "querying from {ConnectionStringName} using custom query",
                ConnectionString.Name);
        }

        protected override void IncrementCounter()
        {
            CounterCollection.IncrementCounter("db records read", 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name + "/custom query", 1);
            Context.CounterCollection.IncrementCounter("db records read - " + ConnectionString.Name + "/custom query/" + Name, 1);
        }
    }
}