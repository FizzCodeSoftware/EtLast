namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public abstract class AbstractOperationProcess : IOperationProcess
    {
        protected List<IRow> Rows { get; private set; } = new List<IRow>();
        protected int RowsAdded { get; private set; }

        private readonly ReaderWriterLockSlim _rowsLock = new ReaderWriterLockSlim();
        protected IRowQueue RowQueue { get; private set; }

        private long _activeRowCount = 0;
        protected long ActiveRowCount => Interlocked.Read(ref _activeRowCount);

        protected readonly CancellationTokenSource WorkerCancellationTokenSource = new CancellationTokenSource();
        private readonly List<IRowOperation> _operations = new List<IRowOperation>();

        public string Name { get; }
        public IEtlContext Context { get; }
        public IProcess Caller { get; protected set; }

        protected abstract BasicOperationProcessConfiguration BasicConfiguration { get; }
        protected abstract bool KeepOrder { get; }

        public List<IRowOperation> Operations
        {
            get => _operations;
            set => SetOperations(value);
        }

        private readonly List<Thread> _workerThreads = new List<Thread>();

        private void SetOperations(List<IRowOperation> operations)
        {
            foreach (var op in _operations)
            {
                op.SetParentGroup(null, null, 0);
            }

            _operations.Clear();

            foreach (var op in operations)
            {
                AddOperation(op);
            }
        }

        public IProcess InputProcess { get; set; }

        protected AbstractOperationProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name ?? nameof(OperationProcess);
        }

        public T AddOperation<T>(T operation)
            where T : IRowOperation
        {
            operation.SetParent(this, _operations.Count);

            if (_operations.Count > 0)
            {
                _operations[_operations.Count - 1].SetNextOperation(operation);
                operation.SetPrevOperation(_operations[_operations.Count - 1]);
            }

            _operations.Add(operation);

            return operation;
        }

        public void AddRow(IRow row, IRowOperation operation)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    Rows.Add(row);
                    RowsAdded++;
                }
                finally { _rowsLock.ExitWriteLock(); }
            }

            var nextOp = GetNextOp(row);
            if (nextOp != null)
            {
                EnqueueOperation(nextOp, row);

                Interlocked.Increment(ref _activeRowCount);
            }
            else
            {
                row.State = RowState.Finished;
            }

            operation?.Stat.IncrementCounter("rows added", 1);
        }

        public void AddRows(ICollection<IRow> rows, IRowOperation operation)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    Rows.AddRange(rows);
                    RowsAdded += rows.Count;
                }
                finally { _rowsLock.ExitWriteLock(); }
            }

            var addedCount = 0;
            var rowCount = 0;
            foreach (var row in rows)
            {
                rowCount++;
                var nextOp = GetNextOp(row);
                if (nextOp != null)
                {
                    // EnqueueOperation(nextOp, row);
                    row.CurrentOperation = nextOp;
                    RowQueue.AddRowNoSignal(row);

                    addedCount++;
                }
                else
                {
                    row.State = RowState.Finished;
                }
            }

            if (addedCount > 0) RowQueue.Signal();

            Interlocked.Add(ref _activeRowCount, addedCount);
            if (operation != null) operation.Stat.IncrementCounter("rows added", rowCount);
        }

        public IRowOperation GetNextOp(IRow row)
        {
            if (row.State != RowState.Normal) return null;

            IRowOperation nextOp = null;
            if (row.CurrentOperation != null)
            {
                if (row.CurrentOperation.Index < _operations.Count - 1)
                {
                    nextOp = _operations[row.CurrentOperation.Index + 1];
                }
            }
            else
            {
                nextOp = _operations.FirstOrDefault();
            }

            return nextOp;
        }

        private bool TestDone()
        {
            var done = ActiveRowCount == 0;
            var terminated = Context.CancellationTokenSource.IsCancellationRequested;
            if (done || terminated)
            {
                WorkerCancellationTokenSource.Cancel();
                return true;
            }

            return false;
        }

        private void WipeAndGet(List<IRow> finishedCollection, Stopwatch swProcessing, ref int wipedRowCount)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    if (!KeepOrder)
                    {
                        var sw = Stopwatch.StartNew();
                        var hs = new HashSet<IRow>(Rows.Where(x => x.State == RowState.Finished));
                        if (hs.Count > 0)
                        {
                            finishedCollection.AddRange(hs);

                            var count = Rows.Count;
                            Rows = Rows.Where(x => !hs.Contains(x) && x.State != RowState.Removed).ToList();
                            if (Rows.Count != count)
                            {
                                wipedRowCount += count - Rows.Count;
                                Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows without keeping order in {Elapsed}, average speed is {AvgWipeSpeed} msec/Krow", count - Rows.Count, count, sw.Elapsed, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                            }
                        }
                    }
                    else
                    {
                        var lastRemoveableIndex = -1;
                        for (var i = 0; i < Rows.Count; i++)
                        {
                            if (Rows[i].State == RowState.Finished)
                            {
                                lastRemoveableIndex = i;
                                finishedCollection.Add(Rows[i]);
                            }
                            else if (Rows[i].State == RowState.Removed)
                            {
                                lastRemoveableIndex = i;
                            }
                            else
                            {
                                break;
                            }
                        }

                        if (lastRemoveableIndex > -1)
                        {
                            Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows while keeping order, average speed is {AvgWipeSpeed} msec/Krow", lastRemoveableIndex + 1, Rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                            Rows.RemoveRange(0, lastRemoveableIndex + 1);
                        }
                    }
                }
                finally
                {
                    _rowsLock.ExitWriteLock();
                }
            }
        }

        private void Wipe(Stopwatch swProcessing, ref int wipedRowCount)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    var count = Rows.Count;
                    Rows = Rows.Where(x => x.State == RowState.Normal).ToList();
                    if (Rows.Count != count)
                    {
                        wipedRowCount += count - Rows.Count;
                        Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows, average speed is {AvgWipeSpeed} msec/Krow", count - Rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                    }
                }
                finally
                {
                    _rowsLock.ExitWriteLock();
                }
            }
        }

        private void CreateRowQueue(Type type)
        {
            RowQueue = (IRowQueue)Activator.CreateInstance(type);
        }

        private void PrepareOperations()
        {
            try
            {
                foreach (var op in Operations)
                {
                    PrepareOperation(op);
                }
            }
            catch (Exception ex)
            {
                Context.AddException(this, ex);
            }
        }

        private void PrepareOperation(IRowOperation op)
        {
            try
            {
                op.Prepare();
                if (op is IOperationGroup group)
                {
                    foreach (var childOp in group.Then)
                    {
                        PrepareOperation(childOp);
                    }

                    foreach (var childOp in group.Else)
                    {
                        PrepareOperation(childOp);
                    }
                }
            }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new OperationExecutionException(this, op, "exception raised during Prepare()", ex); }
        }

        private void ShutdownOperations()
        {
            try
            {
                foreach (var op in Operations)
                {
                    ShutdownOperation(op);
                }
            }
            catch (Exception ex)
            {
                Context.AddException(this, ex);
            }
        }

        private void ShutdownOperation(IRowOperation op)
        {
            try
            {
                op.Shutdown();
                if (op is IOperationGroup group)
                {
                    foreach (var childOp in group.Then)
                    {
                        ShutdownOperation(childOp);
                    }

                    foreach (var childOp in group.Else)
                    {
                        ShutdownOperation(childOp);
                    }
                }
            }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new OperationExecutionException(this, op, "exception raised during Shutdown()", ex); }
        }

        public void EnqueueOperation(IRowOperation operation, IRow row)
        {
            row.CurrentOperation = operation;
            RowQueue.AddRow(row);
        }

        public void RemoveRow(IRow row, IRowOperation operation)
        {
            row.State = RowState.Removed;
            operation.Stat.IncrementCounter("rows removed", 1);
        }

        public void RemoveRows(IEnumerable<IRow> rows, IRowOperation operation)
        {
            var n = 0;
            foreach (var row in rows)
            {
                row.State = RowState.Removed;
                n++;
            }

            operation.Stat.IncrementCounter("rows removed", n);
        }

        public void FlagRowAsFinished(IRow row)
        {
            Interlocked.Decrement(ref _activeRowCount);

            // do not overwrite Removed with Finished!
            if (row.State == RowState.Normal)
            {
                row.State = RowState.Finished;
            }
        }

        private void LogStats()
        {
            var sb = new System.Text.StringBuilder();
            foreach (var op in Operations)
            {
                LogOpStat(op, sb);
            }
        }

        private void LogOpStat(IRowOperation op, System.Text.StringBuilder sb)
        {
            var counters = op.Stat.Counters.OrderBy(x => x.Key).ToList();
            if (counters.Count == 0) return;

            sb.Append("stats of " + op.Name);
            foreach (var kvp in counters)
            {
                sb.Append(" [" + kvp.Key + " = {" + kvp.Key.Replace(" ", "_") + "}]");
            }

            Context.Log(LogSeverity.Debug, this, sb.ToString(), counters.Select(x => (object)x.Value).ToArray());
            sb.Clear();

            if (op is IOperationGroup group)
            {
                foreach (var cop in group.Then.Concat(group.Else))
                {
                    LogOpStat(cop, sb);
                }
            }
        }

        public void EvaluateWithoutResult(IProcess caller = null)
        {
            Caller = caller;

            Validate();
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            var sw = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "started");

            CreateRowQueue(BasicConfiguration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            CreateWorkers();
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

            var swLoop = Stopwatch.StartNew();
            var sourceRows = InputProcess.Evaluate(this);
            var buffer = new List<IRow>();
            var inputRowCount = 0;
            var wipedRowCount = 0;

            var swProcessing = Stopwatch.StartNew();
            foreach (var row in sourceRows)
            {
                row.CurrentOperation = null;
                row.State = RowState.Normal;

                inputRowCount++;
                buffer.Add(row);

                if (buffer.Count > BasicConfiguration.InputBufferSize)
                {
                    AddRows(buffer, null);
                    buffer.Clear();

                    Stopwatch swSleep = null;
                    while (true)
                    {
                        if (swLoop.ElapsedMilliseconds >= BasicConfiguration.MainLoopDelay)
                        {
                            Wipe(swProcessing, ref wipedRowCount);
                            swLoop.Restart();
                        }

                        if (ActiveRowCount <= BasicConfiguration.ThrottlingLimit) break;

                        if (swSleep != null)
                        {
                            if (swSleep.ElapsedMilliseconds >= BasicConfiguration.ThrottlingMaxSleep) break;
                        }
                        else
                        {
                            swSleep = Stopwatch.StartNew();
                        }

                        Thread.Sleep(BasicConfiguration.ThrottlingSleepResolution);
                    }

                    if (swSleep != null)
                    {
                        Context.Log(LogSeverity.Verbose, this, "slept {Elapsed} to lower active row count to {ActiveRowCount}, input buffer: {InputBufferCount}/{InputBufferSize}", swSleep.Elapsed, ActiveRowCount, buffer.Count, BasicConfiguration.InputBufferSize);
                    }
                }

                if (Context.CancellationTokenSource.IsCancellationRequested) break;
            }

            if (buffer.Count > 0)
            {
                AddRows(buffer, null);
                buffer.Clear();
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows in {Elapsed}", inputRowCount, sw.Elapsed);

            var loopIndex = 0;
            while (true)
            {
                Wipe(swProcessing, ref wipedRowCount);

                if (TestDone()) break;

                if (loopIndex > 100)
                {
                    Thread.Sleep(BasicConfiguration.MainLoopDelay);
                }
                else
                {
                    loopIndex++;
                    Thread.Sleep(10);
                }
            }

            WaitForWorkerThreads();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", sw.Elapsed);
        }

        public IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            Validate();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            var sw = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "started");

            CreateRowQueue(BasicConfiguration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            CreateWorkers();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            var resultCount = 0;
            Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

            var swLoop = Stopwatch.StartNew();
            var sourceRows = InputProcess.Evaluate(this);
            var buffer = new List<IRow>();
            var inputRowCount = 0;
            var wipedRowCount = 0;

            var swProcessing = Stopwatch.StartNew();
            var swSleep = new Stopwatch();

            var finished = new List<IRow>();

            foreach (var row in sourceRows)
            {
                row.CurrentOperation = null;
                row.State = RowState.Normal;

                inputRowCount++;
                buffer.Add(row);

                if (buffer.Count >= BasicConfiguration.InputBufferSize)
                {
                    AddRows(buffer, null);
                    buffer.Clear();

                    while (true)
                    {
                        if (swLoop.ElapsedMilliseconds >= BasicConfiguration.MainLoopDelay)
                        {
                            WipeAndGet(finished, swProcessing, ref wipedRowCount);
                            swLoop.Restart();

                            if (finished.Count > 0)
                            {
                                resultCount += finished.Count;
                                foreach (var finishedRow in finished)
                                {
                                    yield return finishedRow;
                                }

                                Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows of {OutputRowCount} in total, read input rows: {InputRowCount}, active rows: {ActiveRowCount}", finished.Count, resultCount, inputRowCount, ActiveRowCount);

                                finished.Clear();
                            }
                        }

                        if (ActiveRowCount <= BasicConfiguration.ThrottlingLimit) break;

                        if (swSleep.IsRunning)
                        {
                            if (swSleep.ElapsedMilliseconds >= BasicConfiguration.ThrottlingMaxSleep) break;
                        }
                        else
                        {
                            swSleep.Restart();
                        }

                        Thread.Sleep(BasicConfiguration.ThrottlingSleepResolution);
                    }

                    if (swSleep.IsRunning)
                    {
                        Context.Log(LogSeverity.Verbose, this, "slept {Sleep} to lower active row count to {ActiveRowCount}", swSleep.Elapsed, ActiveRowCount);
                        swSleep.Stop();
                    }
                }

                if (Context.CancellationTokenSource.IsCancellationRequested) break;
            }

            if (buffer.Count > 0)
            {
                AddRows(buffer, null);
                buffer.Clear();
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows in {Elapsed}", inputRowCount, sw.Elapsed);

            var loopIndex = 0;
            while (true)
            {
                WipeAndGet(finished, swProcessing, ref wipedRowCount);

                if (finished.Count > 0)
                {
                    resultCount += finished.Count;
                    foreach (var finishedRow in finished)
                    {
                        yield return finishedRow;
                    }

                    Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows of {OutputRowCount} in total, active rows: {ActiveRowCount}", finished.Count, resultCount, ActiveRowCount);

                    finished.Clear();
                }

                if (TestDone()) break;

                if (loopIndex > 100)
                {
                    Thread.Sleep(BasicConfiguration.MainLoopDelay);
                }
                else
                {
                    loopIndex++;
                    Thread.Sleep(10);
                }
            }

            // safely ignore BasicConfiguration.KeepOrder because _rows is already ordered
            // so all remaining (finished) items are ordered
            // todo: test because this is optimistic concurrency
            finished.AddRange(Rows.Where(x => x.State == RowState.Finished));
            if (finished.Count > 0)
            {
                foreach (var finishedRow in finished)
                {
                    yield return finishedRow;
                }

                Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows", Rows.Count);
                Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows", finished.Count);
                finished.Clear();
            }

            Rows.Clear();

            WaitForWorkerThreads();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished and retuned {RowCount} rows of {AllRowCount} rows in {Elapsed}", resultCount, RowsAdded, sw.Elapsed);
        }

        protected abstract void CreateWorkers();

        protected abstract void Validate();

        private void WaitForWorkerThreads()
        {
            foreach (var thread in _workerThreads)
            {
                thread.Join();
            }
        }

        protected void CreateWorkerThreads(int count, Type workerType)
        {
            for (var i = 1; i <= count; i++)
            {
                var worker = (IOperationProcessWorker)Activator.CreateInstance(workerType);
                var thread = new Thread(tran =>
                {
                    Transaction.Current = tran as Transaction;
                    var rowsConsumer = RowQueue.GetConsumer(WorkerCancellationTokenSource.Token);

                    try
                    {
                        worker.Process(rowsConsumer, this, WorkerCancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        Context.AddException(this, ex);
                    }

                    Context.Log(LogSeverity.Debug, this, "worker thread ended");
                });

                thread.Start(Transaction.Current);
                _workerThreads.Add(thread);
            }
        }
    }
}