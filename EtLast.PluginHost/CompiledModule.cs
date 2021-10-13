namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using System.Runtime.Loader;
    using FizzCode.EtLast;

    internal class CompiledModule
    {
        public EtlModuleConfiguration Configuration { get; init; }
        public List<IEtlPlugin> Plugins { get; init; }
        public List<IEtlPlugin> EnabledPlugins { get; init; }
        public AssemblyLoadContext LoadContext { get; init; }
    }
}