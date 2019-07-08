namespace FizzCode.EtLast.AdoNet
{
    using System.Configuration;

    public class AdoNetSqlStatementDebugEvent
    {
        public IBaseOperation Operation { get; set; }
        public IJob Job { get; set; }
        public ConnectionStringSettings ConnectionStringSettings { get; set; }
        public string SqlStatement { get; set; }
        public string CompiledSqlStatement { get; set; }
    }
}