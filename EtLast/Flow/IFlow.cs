namespace FizzCode.EtLast;

public interface IFlow
{
    public IEtlContext Context { get; }

    public IFlow If(Func<bool> test, Action action);

    public IFlow ExecuteSequence(Func<IFluentSequenceBuilder, ISequenceBuilder> sequenceBuilder);
    public IFlow ExecuteSequenceAndTakeRows(out List<ISlimRow> rows, Func<IFluentSequenceBuilder, ISequenceBuilder> sequenceBuilder);

    public IFlow ExecuteProcess<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ExecuteProcess<T>(out T createdProcess, Func<T> processCreator) where T : IProcess;
    public IFlow ExecuteProcessWithResult<TProcess, TResult>(out TResult result, Func<TProcess> processCreator) where TProcess : IProcessWithResult<TResult>;
    public IFlow ExecuteForEach<TElement>(IEnumerable<TElement> elements, Action<TElement> action);
    public IFlow ExecuteForEachIsolated<TElement>(IEnumerable<TElement> elements, Action<TElement, IFlow> action);

    public IFlow CaptureValue<T>(out T variable, Func<T> calculatorFunc);
    public IFlow InitializeVariable<T>(out Variable<T> variable, Func<T> valueFunc, string name = null);

    /// <summary>
    /// Only continue the execution with the next element when the supplied action returns true.
    /// </summary>
    public IFlow ExecuteForEachConditional<TElement>(IEnumerable<TElement> elements, Func<TElement, bool> action);

    /// <summary>
    /// Only continue the execution with the next element when the supplied action returns true.
    /// </summary>
    public IFlow ExecuteForEachIsolatedConditional<TElement>(IEnumerable<TElement> elements, Func<TElement, IFlow, bool> action);

    public IFlow Isolate(Action<IFlow> builder);
    public IFlow TransactionScope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information);
    public IFlow HandleError<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();

    public IReadOnlyFlowState State { get; }
}