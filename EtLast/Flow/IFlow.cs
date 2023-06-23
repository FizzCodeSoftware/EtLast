namespace FizzCode.EtLast;

public interface IFlow
{
    public IFlow ContinueWithSequence<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder) where T : ISequence;
    public IFlow ContinueWithProcess<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ContinueWithProcess<T>(out T createdProcess, Func<T> processCreator) where T : IProcess;
    public IFlow IsolateFlow(Action<IFlow> builder);
    public IFlow TransactionScope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information);
    public IFlow HandleError<T>(Func<T> processCreator) where T : IProcess;
    public IFlow ThrowOnError();

    public IReadOnlyFlowState State { get; }
}