namespace FizzCode.EtLast;

public interface ISequence : IProcess
{
    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller);
    public int CountRowsAndReleaseOwnership(IProcess caller);

    public IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller, Pipe pipe);
    public IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller, Pipe pipe);
    public int CountRowsAndReleaseOwnership(IProcess caller, Pipe pipe);
}