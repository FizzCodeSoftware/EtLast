namespace FizzCode.EtLast.AdoNet
{
    using FizzCode.DbTools.Configuration;

    public class AdoNetSqlStatementDebugEvent
    {
        public IBaseOperation Operation { get; set; }
        public IJob Job { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public string SqlStatement { get; set; }
        public string CompiledSqlStatement { get; set; }
    }
}