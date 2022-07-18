namespace FizzCode.EtLast;

public sealed class ResilientSqlScopeProcessBuilder
{
    public ResilientSqlScope Scope { get; init; }
    public List<IJob> Processes { get; } = new List<IJob>();

    public ResilientSqlScopeProcessBuilder Add(IJob process)
    {
        Processes.Add(process);

        return this;
    }
}
