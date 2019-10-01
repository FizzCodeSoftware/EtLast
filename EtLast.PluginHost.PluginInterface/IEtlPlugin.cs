namespace FizzCode.EtLast
{
    using System;
    using Serilog;

    public interface IEtlPlugin
    {
        ModuleConfiguration ModuleConfiguration { get; }
        IEtlContext Context { get; }
        void Init(ILogger logger, ILogger opsLogger, ModuleConfiguration moduleConfiguration, TimeSpan transactionScopeTimeout);
        void BeforeExecute();
        void AfterExecute();
        void Execute();
    }
}