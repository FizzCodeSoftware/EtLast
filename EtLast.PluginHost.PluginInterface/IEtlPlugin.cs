namespace FizzCode.EtLast
{
    using System;
    using Microsoft.Extensions.Configuration;
    using Serilog;

    public interface IEtlPlugin
    {
        IEtlContext Context { get; }
        void Init(ILogger logger, ILogger opsLogger, IConfigurationRoot moduleConfiguration, string moduleFolder, TimeSpan transactionScopeTimeout);
        void BeforeExecute();
        void AfterExecute();
        void Execute();
    }
}