namespace FizzCode.EtLast;

public interface IRowQueue : IDisposable
{
    void AddRow(IRow row);

    void AddRowNoSignal(IRow row);
    void Signal();

    void SignalNoMoreRows();
    IEnumerable<IRow> GetConsumer(CancellationToken token);
}
