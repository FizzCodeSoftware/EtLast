namespace FizzCode.EtLast.PluginHost
{
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public interface IExecutionContextListener : IEtlContextListener
    {
        bool Init(IExecutionContext executionContext, IConfigurationSection configurationSection, IConfigurationSecretProtector configurationSecretProtector);
    }
}