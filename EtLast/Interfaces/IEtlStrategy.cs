namespace FizzCode.EtLast
{
    public interface IEtlStrategy : ICaller
    {
        IEtlContext Context { get; }
        void Execute(ICaller caller);
    }
}