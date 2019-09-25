namespace FizzCode.EtLast.AdoNet
{
    using System.Data;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class DatabaseConnection
    {
        public string Key { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public IDbConnection Connection { get; set; }
        public int ReferenceCount { get; set; }
        public bool Failed { get; set; }
        public object Lock { get; set; } = new object();
        public Transaction TransactionWhenCreated { get; set; }
    }
}