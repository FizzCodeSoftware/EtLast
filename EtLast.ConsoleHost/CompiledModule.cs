namespace FizzCode.EtLast;

internal class CompiledModule
{
    public required string Name { get; init; }
    public required string Directory { get; init; }
    public required List<IInstanceArgumentProvider> InstanceArgumentProviders { get; init; }
    public required List<IDefaultArgumentProvider> DefaultArgumentProviders { get; init; }
    public required IStartup Startup { get; init; }
    public required List<Type> TaskTypes { get; init; }
    public required AssemblyLoadContext LoadContext { get; init; }
}
