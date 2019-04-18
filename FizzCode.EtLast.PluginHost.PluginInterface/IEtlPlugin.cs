namespace FizzCode.EtLast
{
    using Serilog;
    using System.Configuration;

    public interface IEtlPlugin
    {
        void Init(ILogger logger, ILogger opsLogger, Configuration configuration, EtlPluginResult pluginResult, string pluginFolder);
        void Execute();
    }
}