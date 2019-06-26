namespace FizzCode.EtLast
{
    using System;
    using System.Configuration;
    using Serilog;

    public interface IEtlPlugin
    {
        void Init(ILogger logger, ILogger opsLogger, Configuration configuration, EtlPluginResult pluginResult, string pluginFolder, TimeSpan transactionScopeTimeout);
        void Execute();
    }
}