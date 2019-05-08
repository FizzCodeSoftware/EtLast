namespace FizzCode.EtLast
{
    using System.Configuration;
    using Serilog;

    public interface IEtlPlugin
    {
        void Init(ILogger logger, ILogger opsLogger, Configuration configuration, EtlPluginResult pluginResult, string pluginFolder);
        void Execute();
    }
}