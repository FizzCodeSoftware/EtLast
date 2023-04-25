namespace FizzCode.EtLast;

public interface ISequence : IProcess
{
    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller);
    public int CountRowsAndReleaseOwnership(IProcess caller);

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, FlowState flowState);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, FlowState flowState);
    public int CountRowsAndReleaseOwnership(IProcess caller, FlowState flowState);
}