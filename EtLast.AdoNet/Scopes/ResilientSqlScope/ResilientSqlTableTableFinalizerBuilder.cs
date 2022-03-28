namespace FizzCode.EtLast;

public sealed class ResilientSqlTableTableFinalizerBuilder
{
    public ResilientTableBase Table { get; init; }
    public List<IExecutable> Finalizers { get; } = new List<IExecutable>();

    public ResilientSqlTableTableFinalizerBuilder Add(IExecutable process)
    {
        Finalizers.Add(process);
        return this;
    }
}
