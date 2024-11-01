namespace FizzCode.EtLast;

internal class CompiledModule
{
    public required string Name { get; init; }
    public required string Directory { get; init; }
    public required List<InstanceArgumentProvider> InstanceArgumentProviders { get; init; }
    public required List<ArgumentProvider> DefaultArgumentProviders { get; init; }
    public required StartupDelegate Startup { get; init; }
    public required List<Type> TaskTypes { get; init; }
    public required List<Type> PreCompiledTaskTypes { get; init; }
    public required AssemblyLoadContext LoadContext { get; init; }
}
