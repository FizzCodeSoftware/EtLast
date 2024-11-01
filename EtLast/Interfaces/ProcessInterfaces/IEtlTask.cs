namespace FizzCode.EtLast;

public interface IEtlTask : IProcess
{
    public IExecutionStatistics Statistics { get; }
    public IEnumerable<IIoCommandCounter> IoCommandCounters { get; }

    public Action<IFlow> ExecuteBefore { get; init; }
    public Action<IFlow> ExecuteAfter { get; init; }

    public void ValidateParameters();
}