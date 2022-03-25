namespace FizzCode.EtLast;

public interface IExecutable : IProcess
{
    void Execute(IProcess caller);
}
