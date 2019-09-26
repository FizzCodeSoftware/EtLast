namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using System.Reflection;
    using System.Runtime.Loader;
    using FizzCode.EtLast;

    internal class Module
    {
        public ModuleConfiguration ModuleConfiguration { get; set; }
        public List<IEtlPlugin> Plugins { get; set; }
        public List<IEtlPlugin> EnabledPlugins { get; set; }
        public Assembly Assembly { get; set; }
        public AssemblyLoadContext AssemblyLoadContext { get; set; }
    }
}