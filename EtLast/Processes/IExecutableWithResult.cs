namespace FizzCode.EtLast;

public interface IExecutableWithResult<T> : IProcess
{
    T Execute(IProcess caller);
}
