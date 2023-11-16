namespace FizzCode.EtLast;

public sealed class ResilientSqlScopeProcessBuilder
{
    public required ResilientSqlScope Scope { get; init; }
    public List<IProcess> Jobs { get; } = [];

    public ResilientSqlScopeProcessBuilder Add(IProcess process)
    {
        Jobs.Add(process);

        return this;
    }
}
