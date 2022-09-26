namespace FizzCode.EtLast;

public interface IEtlFlow : IEtlTask
{
    public IPipeStarter NewPipe();
}