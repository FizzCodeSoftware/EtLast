namespace FizzCode.EtLast;

public interface IJob : IProcess
{
    void Execute(IProcess caller);
}