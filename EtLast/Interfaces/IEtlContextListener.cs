namespace FizzCode.EtLast;

public interface IEtlContextListener
{
    void Start();

    void OnLog(LogSeverity severity, bool forOps, string transactionId, IProcess process, string text, params object[] args);
    void OnCustomLog(bool forOps, string fileName, IProcess process, string text, params object[] args);
    void OnException(IProcess process, Exception exception);

    void OnRowCreated(IReadOnlyRow row);
    void OnRowOwnerChanged(IReadOnlyRow row, IProcess previousProcess, IProcess currentProcess);
    void OnRowValueChanged(IReadOnlyRow row, params KeyValuePair<string, object>[] values);
    void OnSinkStarted(long sinkId, string location, string path, string sinkFormat, Type sinkWriter);
    void OnWriteToSink(IReadOnlyRow row, long sinkId);

    void OnProcessInvocationStart(IProcess process);
    void OnProcessInvocationEnd(IProcess process);
    void OnContextIoCommandStart(IProcess process, IoCommand ioCommand);
    void OnContextIoCommandEnd(IProcess process, IoCommand ioCommand);

    void OnContextClosed(); // all pending state must be flushed in this method
}
