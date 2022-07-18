namespace FizzCode.EtLast;

public interface IJobWithResult<T> : IProcess, IJob
{
    T ExecuteWithResult(IProcess caller);
}