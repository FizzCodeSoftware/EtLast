namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;

    public class DefaultRowQueue : IRowQueue
    {
        private readonly AutoResetEvent _newRowEvent = new(false);
        private readonly ManualResetEvent _noMoreRowsEvent = new(false);
        private readonly ConcurrentQueue<IRow> _queue = new();

        public void AddRow(IRow row)
        {
            _queue.Enqueue(row);
            _newRowEvent.Set();
        }

        public void AddRowNoSignal(IRow row)
        {
            _queue.Enqueue(row);
        }

        public void Signal()
        {
            _newRowEvent.Set();
        }

        public void SignalNoMoreRows()
        {
            _noMoreRowsEvent.Set();
        }

        public IEnumerable<IRow> GetConsumer(CancellationToken token)
        {
            var waitHandles = new[] { token.WaitHandle, _newRowEvent, _noMoreRowsEvent };

            while (true)
            {
                if (token.IsCancellationRequested)
                    yield break;

                IRow row;
                while (!_queue.TryDequeue(out row))
                {
                    WaitHandle.WaitAny(waitHandles);
                    if (token.IsCancellationRequested)
                        yield break;

                    if (_noMoreRowsEvent.WaitOne(0))
                    {
                        while (_queue.TryDequeue(out row))
                            yield return row;

                        yield break;
                    }
                }

                yield return row;
            }
        }

        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _newRowEvent.Dispose();
                    _noMoreRowsEvent.Dispose();
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