namespace FizzCode.EtLast
{
    public interface ISplitProcess : IProcess
    {
        IProcess InputProcess { get; set; }
    }
}