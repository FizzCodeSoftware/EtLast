namespace FizzCode.EtLast;

public sealed class ResilientSqlTableTableFinalizerBuilder
{
    public ResilientTableBase Table { get; init; }
    public List<IJob> Finalizers { get; } = new List<IJob>();

    public ResilientSqlTableTableFinalizerBuilder Add(IJob process)
    {
        Finalizers.Add(process);
        return this;
    }
}
