namespace FizzCode.EtLast;

public interface ISequence : IJob
{
    IEnumerable<IRow> TakeRowsAndTransferOwnership(IProcess caller);
    IEnumerable<ISlimRow> TakeRowsAndReleaseOwnership(IProcess caller);
    int CountRowsAndReleaseOwnership(IProcess caller);
}