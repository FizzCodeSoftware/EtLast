namespace FizzCode.EtLast
{
    public interface IDeferredRowOperation : IRowOperation
    {
        int BatchSize { get; }
    }
}