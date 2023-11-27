namespace FizzCode.EtLast;

public interface IProcessWithResult<TResult> : IProcess
{
    TResult ExecuteWithResult(ICaller caller, FlowState flowState = null);
}