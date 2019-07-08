namespace FizzCode.EtLast
{
    using System;
    using System.Configuration;
    using Serilog;

    public interface IEtlPlugin
    {
        IEtlContext Context { get; }
        void Init(ILogger logger, ILogger opsLogger, Configuration configuration, string pluginFolder, TimeSpan transactionScopeTimeout);
        void BeforeExecute();
        void AfterExecute();
        void Execute();
    }
}