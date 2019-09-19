namespace FizzCode.EtLast
{
    public interface IFinalProcess : IProcess
    {
        void EvaluateWithoutResult(ICaller caller = null);
    }
}