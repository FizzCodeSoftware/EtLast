namespace FizzCode.EtLast;

public interface IFluentProcessBuilder : IProcessBuilder
{
    IProducer Result { get; set; }
    IFluentProcessMutatorBuilder ReadFrom(IProducer process);
}
