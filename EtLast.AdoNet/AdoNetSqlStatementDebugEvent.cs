namespace FizzCode.EtLast.AdoNet
{
    using FizzCode.DbTools.Configuration;

    public class AdoNetSqlStatementDebugEvent
    {
        public IOperation Operation { get; set; }
        public IProcess Process { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public string SqlStatement { get; set; }
        public string CompiledSqlStatement { get; set; }
    }
}