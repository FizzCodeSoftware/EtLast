namespace FizzCode.EtLast;

public interface IJob : IProcess
{
    void Execute(IProcess caller);
    void Execute(IProcess caller, ProcessInvocationContext invocationContext);
}