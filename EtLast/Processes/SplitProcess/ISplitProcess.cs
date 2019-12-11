namespace FizzCode.EtLast
{
    public interface ISplitProcess : IProcess
    {
        IEvaluable InputProcess { get; set; }
    }
}