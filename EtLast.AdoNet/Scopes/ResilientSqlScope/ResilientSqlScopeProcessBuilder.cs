namespace FizzCode.EtLast;

public sealed class ResilientSqlScopeProcessBuilder
{
    public ResilientSqlScope Scope { get; init; }
    public List<IJob> Jobs { get; } = new List<IJob>();

    public ResilientSqlScopeProcessBuilder Add(IJob process)
    {
        Jobs.Add(process);

        return this;
    }
}
