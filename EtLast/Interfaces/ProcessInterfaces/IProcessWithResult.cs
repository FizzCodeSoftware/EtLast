namespace FizzCode.EtLast;

public interface IProcessWithResult<TResult> : IProcess
{
    TResult ExecuteWithResult(IProcess caller);
    TResult ExecuteWithResult(IProcess caller, FlowState flowState);
}