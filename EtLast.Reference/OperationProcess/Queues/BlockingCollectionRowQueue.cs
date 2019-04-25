namespace FizzCode.EtLast
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;

    public class BlockingCollectionRowQueue : IRowQueue
    {
        private readonly BlockingCollection<IRow> _collection = new BlockingCollection<IRow>();

        public void AddRow(IRow row)
        {
            _collection.Add(row);
        }

        public void AddRowNoSignal(IRow row)
        {
            _collection.Add(row);
            // blocking collection does not support this pattern
        }

        public void Signal()
        {
            // blocking collection does not support this pattern
        }

        public void SignalNoMoreRows()
        {
            _collection.CompleteAdding();
        }

        public IEnumerable<IRow> GetConsumer(CancellationToken token)
        {
            return _collection.GetConsumingEnumerable(token);
        }
    }
}
