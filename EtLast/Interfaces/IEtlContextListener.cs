namespace FizzCode.EtLast;

public interface IEtlContextListener
{
    void Start();

    void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args);
    void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args);
    void OnException(IProcess process, Exception exception);

    void OnSinkStarted(IProcess process, Sink sink);
    void OnWriteToSink(Sink sink, IReadOnlyRow row);

    void OnProcessStart(IProcess process);
    void OnProcessEnd(IProcess process);
    void OnContextIoCommandStart(IoCommand ioCommand);
    void OnContextIoCommandEnd(IoCommand ioCommand);

    void OnContextClosed(); // all pending state must be flushed in this method
}
