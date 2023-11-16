namespace FizzCode.EtLast;

public sealed class ResilientSqlTableTableFinalizerBuilder
{
    public required ResilientTableBase Table { get; init; }
    public List<IProcess> Finalizers { get; } = [];

    public ResilientSqlTableTableFinalizerBuilder Add(IProcess process)
    {
        Finalizers.Add(process);
        return this;
    }
}
