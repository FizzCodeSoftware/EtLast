namespace FizzCode.EtLast
{
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public interface IEtlSessionListener : IEtlContextListener
    {
        bool Init(IEtlSession session, IConfigurationSection configurationSection, IConfigurationSecretProtector configurationSecretProtector);
    }
}