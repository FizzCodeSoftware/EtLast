namespace FizzCode.EtLast;

public interface IFlow
{
    // todo: support transaction scope suppression for process creators
    // todo: support transaction scoped subflows
    // todo: consider providing FluentProcessBuilder instead of a process creator func
    // todo: when finished, get rid of BasicScope
    // todo: when finished, consider RelientScope JobCreator refactors... GO WILD!
    public IFlow OnSuccess<T>(Func<T> processCreator) where T : IProcess;
    public IFlow OnSuccess<T>(out T result, Func<T> processCreator) where T : IProcess;
    public IFlow RunIsolated(Action<IsolatedFlowContext> starter);
    public IFlow HandleErrorIsolated<T>(Func<FlowErrorContext, T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();
}