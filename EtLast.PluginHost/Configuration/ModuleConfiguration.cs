namespace FizzCode.EtLast.PluginHost
{
    using Microsoft.Extensions.Configuration;

    internal class ModuleConfiguration
    {
        public string ModuleName { get; set; }
        public string ModuleFolder { get; set; }
        public string ConfigurationFileName { get; set; }
        public IConfigurationRoot Configuration { get; set; }
    }
}