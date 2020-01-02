namespace FizzCode.EtLast
{
    using System;

    public interface IEtlPlugin
    {
        ModuleConfiguration ModuleConfiguration { get; }
        IEtlContext Context { get; }
        string Name { get; }

        void Init(IEtlPluginLogger logger, ModuleConfiguration moduleConfiguration, TimeSpan transactionScopeTimeout, StatCounterCollection moduleStatCounterCollection);
        void BeforeExecute();
        void AfterExecute();
        void Execute();
    }
}