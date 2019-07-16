namespace FizzCode.EtLast
{
    public interface IEtlStrategy
    {
        void Execute(IEtlContext context);
    }
}