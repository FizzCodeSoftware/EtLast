namespace FizzCode.EtLast
{
    public interface IFluentProcessBuilder : IProcessBuilder
    {
        IEvaluable Result { get; set; }
        IFluentProcessMutatorBuilder ReadFrom(IEvaluable process);
    }
}