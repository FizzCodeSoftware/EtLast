namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public class EtlModuleConfiguration
    {
        public string ModuleName { get; init; }
        public string ModuleFolder { get; init; }
        public string ConfigurationFileName { get; init; }

        public IConfigurationRoot Configuration { get; init; }
        public List<string> EnabledPluginList { get; init; }
        public ConnectionStringCollection ConnectionStrings { get; init; }

        public IConfigurationSecretProtector SecretProtector { get; init; }
    }
}