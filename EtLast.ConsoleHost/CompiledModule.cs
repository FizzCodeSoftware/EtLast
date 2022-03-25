namespace FizzCode.EtLast.ConsoleHost;

using System;
using System.Collections.Generic;
using System.Runtime.Loader;
using FizzCode.EtLast;

internal class CompiledModule
{
    public string Name { get; init; }
    public string Folder { get; init; }
    public List<IInstanceArgumentProvider> InstanceArgumentProviders { get; init; }
    public List<IDefaultArgumentProvider> DefaultArgumentProviders { get; init; }
    public IStartup Startup { get; init; }
    public List<Type> TaskTypes { get; init; }
    public AssemblyLoadContext LoadContext { get; init; }
}
