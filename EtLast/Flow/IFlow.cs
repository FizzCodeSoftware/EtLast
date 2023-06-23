namespace FizzCode.EtLast;

public interface IFlow
{
    public IFlow ExecuteSequence<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder) where T : ISequence;
    public IFlow ExecuteProcess<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ExecuteProcess<T>(out T createdProcess, Func<T> processCreator) where T : IProcess;
    public IFlow ExecuteForEach<TElement>(IEnumerable<TElement> elements, Action<TElement> action);
    public IFlow ExecuteForEachIsolated<TElement>(IEnumerable<TElement> elements, Action<TElement, IFlow> action);
    public IFlow Isolate(Action<IFlow> builder);
    public IFlow TransactionScope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information);
    public IFlow HandleError<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();

    public IReadOnlyFlowState State { get; }
}