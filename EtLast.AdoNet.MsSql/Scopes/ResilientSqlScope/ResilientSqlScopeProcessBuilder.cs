namespace FizzCode.EtLast;

public sealed class ResilientSqlScopeProcessBuilder
{
    public required ResilientSqlScope Scope { get; init; }
    public List<IProcess> Jobs { get; } = new List<IProcess>();

    public ResilientSqlScopeProcessBuilder Add(IProcess process)
    {
        Jobs.Add(process);

        return this;
    }
}
