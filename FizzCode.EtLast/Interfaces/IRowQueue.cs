namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Threading;

    public interface IRowQueue
    {
        void AddRow(IRow row);

        void AddRowNoSignal(IRow row);
        void Signal();

        void SignalNoMoreRows();
        IEnumerable<IRow> GetConsumer(CancellationToken token);
    }
}