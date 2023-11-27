namespace FizzCode.EtLast;

public interface ISequence : IProcess
{
    public IEnumerable<IRow> TakeRowsAndTransferOwnership(ICaller caller, FlowState flowState = null);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(ICaller caller, FlowState flowState = null);
    public int CountRowsAndReleaseOwnership(ICaller caller, FlowState flowState = null);
}