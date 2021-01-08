namespace FizzCode.EtLast.AdoNet
{
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class DatabaseConnection
    {
        public string Key { get; init; }
        public ConnectionStringWithProvider ConnectionString { get; init; }
        public IDbConnection Connection { get; init; }
        public Transaction TransactionWhenCreated { get; init; }
        internal object Lock { get; } = new object();

        public int ReferenceCount { get; internal set; }
        public bool Failed { get; internal set; }

        internal DatabaseConnection()
        {
        }
    }
}