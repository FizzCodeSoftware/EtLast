namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using FizzCode.EtLast;

    internal class Module
    {
        public ModuleConfiguration ModuleConfiguration { get; set; }
        public List<IEtlPlugin> Plugins { get; set; }
        public List<IEtlPlugin> EnabledPlugins { get; set; }
    }
}