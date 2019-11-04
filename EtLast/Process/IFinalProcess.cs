namespace FizzCode.EtLast
{
    public interface IFinalProcess : IProcess
    {
        void EvaluateWithoutResult(IExecutionBlock caller = null);
    }
}