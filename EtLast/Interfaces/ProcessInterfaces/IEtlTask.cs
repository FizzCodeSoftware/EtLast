namespace FizzCode.EtLast;

public interface IEtlTask : IProcess
{
    public IEtlSession Session { get; }

    public IExecutionStatistics Statistics { get; }
    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; }

    public void ValidateParameters();
    public void Execute(IProcess caller, IEtlSession session, ProcessInvocationContext invocationContext);

    public void SetArguments(ArgumentCollection arguments);
}