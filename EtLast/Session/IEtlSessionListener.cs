using FizzCode.LightWeight.Configuration;
using Microsoft.Extensions.Configuration;

namespace FizzCode.EtLast;

public interface IEtlSessionListener : IEtlContextListener
{
    bool Init(IEtlSession session, IConfigurationSection configurationSection, IConfigurationSecretProtector configurationSecretProtector);
}
