namespace FizzCode.EtLast;

public interface IFlowStarter
{
    public IFlow StartWith<T>(Func<T> processCreator) where T : IProcess;
    public IFlow StartWith<T>(out T result, Func<T> processCreator) where T : IProcess;
    public IFlow StartWith<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder) where T : ISequence;
}