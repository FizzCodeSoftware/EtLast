namespace FizzCode.EtLast;

public interface IProcessWithResult<T> : IProcess
{
    T ExecuteWithResult(IProcess caller);
    T ExecuteWithResult(IProcess caller, Pipe pipe);
}