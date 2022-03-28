namespace FizzCode.EtLast;

using Microsoft.Extensions.Configuration;

public interface IEtlSessionListener : IEtlContextListener
{
    bool Init(IEtlSession session, IConfigurationSection configurationSection, IConfigurationSecretProtector configurationSecretProtector);
}
