namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;

    public class BlockingCollectionRowQueue : IRowQueue
    {
        private readonly BlockingCollection<IEtlRow> _collection = new BlockingCollection<IEtlRow>();

        public void AddRow(IEtlRow row)
        {
            _collection.Add(row);
        }

        public void AddRowNoSignal(IEtlRow row)
        {
            _collection.Add(row);
        }

        public void Signal()
        {
        }

        public void SignalNoMoreRows()
        {
            _collection.CompleteAdding();
        }

        public IEnumerable<IEtlRow> GetConsumer(CancellationToken token)
        {
            return _collection.GetConsumingEnumerable(token);
        }

        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _collection.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}