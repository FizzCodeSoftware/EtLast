namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public class ModuleConfiguration
    {
        public string ModuleName { get; set; }
        public string ModuleFolder { get; set; }
        public string ConfigurationFileName { get; set; }

        public IConfigurationRoot Configuration { get; set; }
        public List<string> EnabledPluginList { get; set; }
        public ConnectionStringCollection ConnectionStrings { get; set; }

        public IConfigurationSecretProtector SecretProtector { get; set; }
    }
}