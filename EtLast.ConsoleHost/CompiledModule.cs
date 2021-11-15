namespace FizzCode.EtLast.ConsoleHost
{
    using System.Collections.Generic;
    using System.Runtime.Loader;
    using FizzCode.EtLast;

    internal class CompiledModule
    {
        public string Name { get; init; }
        public string Folder { get; init; }
        public IConfigurationProvider ConfigurationProvider { get; init; }
        public IStartup Startup { get; init; }
        public List<IEtlTask> Tasks { get; init; }
        public AssemblyLoadContext LoadContext { get; init; }
    }
}