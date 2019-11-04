namespace FizzCode.EtLast
{
    public interface IEtlStrategy : IExecutionBlock
    {
        IEtlContext Context { get; }
        void Execute(IExecutionBlock caller);
    }
}