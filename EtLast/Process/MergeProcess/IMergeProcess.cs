namespace FizzCode.EtLast
{
    public interface IMergeProcess : IProcess
    {
        IRowSetMerger Merger { get; }
        void AddInput(IProcess inputProcess);
    }
}