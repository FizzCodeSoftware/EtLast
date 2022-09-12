namespace FizzCode.EtLast;

public interface ISequence : IJob
{
    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller);
    public int CountRowsAndReleaseOwnership(IProcess caller);

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, ProcessInvocationContext invocationContext);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, ProcessInvocationContext invocationContext);
    public int CountRowsAndReleaseOwnership(IProcess caller, ProcessInvocationContext invocationContext);
}