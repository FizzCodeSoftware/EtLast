namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public interface IRowQueue : IDisposable
    {
        void AddRow(IEtlRow row);

        void AddRowNoSignal(IEtlRow row);
        void Signal();

        void SignalNoMoreRows();
        IEnumerable<IEtlRow> GetConsumer(CancellationToken token);
    }
}