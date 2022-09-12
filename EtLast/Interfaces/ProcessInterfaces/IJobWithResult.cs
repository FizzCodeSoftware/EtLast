namespace FizzCode.EtLast;

public interface IJobWithResult<T> : IProcess, IJob
{
    T ExecuteWithResult(IProcess caller);
    T ExecuteWithResult(IProcess caller, ProcessInvocationContext invocationContext);
}