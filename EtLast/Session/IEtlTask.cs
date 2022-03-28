namespace FizzCode.EtLast;

public interface IEtlTask : IProcess
{
    public IEtlSession Session { get; }
    public IExecutionStatistics Statistics { get; }

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; }

    public void ValidateParameters();
    public ProcessResult Execute(IProcess caller, IEtlSession session);
}
