namespace FizzCode.EtLast;

public interface IEtlContextLogger
{
    void Start();

    void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args);
    void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args);
    void OnException(IProcess process, Exception exception);
    void OnContextIoCommandStart(IoCommand ioCommand);
    void OnContextIoCommandEnd(IoCommand ioCommand);

    void OnContextClosed(); // all pending state must be flushed in this method
}