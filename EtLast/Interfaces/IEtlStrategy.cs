namespace FizzCode.EtLast
{
    public interface IEtlStrategy : ICaller
    {
        void Execute(ICaller caller, IEtlContext context);
    }
}