namespace FizzCode.EtLast
{
    public interface ISplitProcess : IEvaluable
    {
        IEvaluable InputProcess { get; set; }
    }
}