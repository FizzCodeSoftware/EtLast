namespace FizzCode.EtLast;

public interface IFlow
{
    // todo: support transaction scope suppression for process creators
    // todo: support transaction scoped subflows
    // todo: consider providing FluentProcessBuilder instead of a process creator func
    // todo: when finished, get rid of BasicScope
    // todo: when finished, consider RelientScope JobCreator refactors... GO WILD!
    public IFlow ContinueWith<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder) where T : ISequence;
    public IFlow ContinueWith<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ContinueWith<T>(out T result, Func<T> processCreator) where T : IProcess;
    public IFlow Isolate(Action<IsolatedFlowContext> starter);
    public IFlow HandleError<T>(Func<FlowErrorContext, T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();
}