namespace FizzCode.EtLast;

public interface IReadOnlyFlowState
{
    IEtlContext Context { get; }
    List<Exception> Exceptions { get; }
    bool Failed { get; }
    bool IsTerminating { get; }
    string StatusToLogString();
}