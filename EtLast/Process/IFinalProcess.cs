namespace FizzCode.EtLast
{
    public interface IFinalProcess : IProcess
    {
        void EvaluateWithoutResult(IProcess caller = null);
    }
}