namespace FizzCode.EtLast;

public interface IPipeStarter
{
    public IPipeBuilder StartWith<T>(T process) where T : IProcess;
    public IPipeBuilder StartWith<T>(out T result, T process) where T : IProcess;
}