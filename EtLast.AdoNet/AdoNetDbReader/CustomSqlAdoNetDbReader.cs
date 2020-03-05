namespace FizzCode.EtLast.AdoNet
{
    public class CustomSqlAdoNetDbReader : AbstractAdoNetDbReader
    {
        public string Sql { get; set; }

        public CustomSqlAdoNetDbReader(ITopic topic, string name)
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

        protected override int RegisterIoCommandStart(string transactionId, int timeout, string statement)
        {
            return Context.RegisterIoCommandStart(this, IoCommandKind.dbRead, ConnectionString.Name, timeout, statement, transactionId, () => Parameters,
                "querying from {ConnectionStringName} using custom query",
                ConnectionString.Name);
        }
    }
}