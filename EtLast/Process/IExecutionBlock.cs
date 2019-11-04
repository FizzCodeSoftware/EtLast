namespace FizzCode.EtLast
{
    public interface IExecutionBlock
    {
        string Name { get; }
        IExecutionBlock Caller { get; }
    }
}