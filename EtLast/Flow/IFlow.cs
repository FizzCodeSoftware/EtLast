namespace FizzCode.EtLast;

public interface IFlow
{
    public IFlow ContinueWith<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder) where T : ISequence;
    public IFlow ContinueWith<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ContinueWith<T>(out T result, Func<T> processCreator) where T : IProcess;
    public IFlow Isolate(Action<IFlow> builder);
    public IFlow Scope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information);
    public IFlow HandleError<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();

    public IReadOnlyFlowState State { get; }
}