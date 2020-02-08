namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class OperationHostProcess : AbstractProcess, IOperationHostProcess
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public IEvaluable InputProcess { get; set; }
        public bool ReadingInput { get; private set; }

        public bool ConsumerShouldNotBuffer => false;
        public OperationHostProcessConfiguration Configuration { get; set; } = new OperationHostProcessConfiguration();

        private List<IRow> _rows = new List<IRow>();
        private int _rowsAdded;

        private readonly ReaderWriterLockSlim _rowsLock = new ReaderWriterLockSlim();
        private IRowQueue _rowQueue;

        private long _activeRowCount = 0;

        private readonly CancellationTokenSource _workerCancellationTokenSource = new CancellationTokenSource();
        private readonly List<IRowOperation> _operations = new List<IRowOperation>();

        public List<IRowOperation> Operations
        {
            get => _operations;
            set => SetOperations(value);
        }

        private Thread _workerThread;

        private void SetOperations(List<IRowOperation> operations)
        {
            foreach (var op in _operations)
            {
                op.SetProcess(null);
            }

            _operations.Clear();

            foreach (var op in operations)
            {
                if (op != null)
                {
                    AddOperation(op);
                }
            }
        }

        public OperationHostProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        public T AddOperation<T>(T operation)
            where T : IRowOperation
        {
            operation.SetProcess(this);

            if (_operations.Count > 0)
            {
                _operations[_operations.Count - 1].SetNextOperation(operation);
                operation.SetPrevOperation(_operations[_operations.Count - 1]);
            }

            _operations.Add(operation);

            CounterCollection.IncrementCounter("operations", 1, true);

            return operation;
        }

        public void AddRow(IRow row, IRowOperation operation)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    _rows.Add(row);
                    _rowsAdded++;
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

            if (operation != null)
            {
                operation.CounterCollection.IncrementCounter("rows created", 1, true);
                CounterCollection.IncrementCounter("rows created by operations", 1, true);
            }
        }

        public void AddRows(ICollection<IRow> rows, IRowOperation operation)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    _rows.AddRange(rows);
                    _rowsAdded += rows.Count;
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
                    row.CurrentOperation = nextOp;
                    _rowQueue.AddRowNoSignal(row);

                    addedCount++;
                }
                else
                {
                    row.State = RowState.Finished;
                }
            }

            if (addedCount > 0)
                _rowQueue.Signal();

            Interlocked.Add(ref _activeRowCount, addedCount);

            if (operation != null)
            {
                operation.CounterCollection.IncrementCounter("rows created", rowCount, true);
                CounterCollection.IncrementCounter("rows created by operations", rowCount, true);
            }
            else
            {
                CounterCollection.IncrementCounter("rows added from source", rowCount, true);
            }
        }

        private IRowOperation GetNextOp(IRow row)
        {
            if (row.State != RowState.Normal)
                return null;

            IRowOperation nextOp = null;
            if (row.CurrentOperation != null)
            {
                nextOp = row.CurrentOperation.NextOperation;
            }
            else
            {
                nextOp = _operations.FirstOrDefault();
            }

            return nextOp;
        }

        private bool TestDone()
        {
            var done = Interlocked.Read(ref _activeRowCount) == 0;
            var terminated = Context.CancellationTokenSource.IsCancellationRequested;
            if (done || terminated)
            {
                _workerCancellationTokenSource.Cancel();
                return true;
            }

            return false;
        }

        private void WipeAndGet(List<IRow> finishedCollection, Stopwatch swProcessing, ref int wipedRowCount)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                CounterCollection.IncrementCounter("wipes", 1, true);

                try
                {
                    if (!Configuration.KeepOrder)
                    {
                        var startedOn = Stopwatch.StartNew();
                        var hs = new HashSet<IRow>(_rows.Where(x => x.State == RowState.Finished));
                        if (hs.Count > 0)
                        {
                            finishedCollection.AddRange(hs);

                            var count = _rows.Count;
                            _rows = _rows.Where(x => !hs.Contains(x) && x.State != RowState.Removed).ToList();
                            if (_rows.Count != count)
                            {
                                wipedRowCount += count - _rows.Count;
                                Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows without keeping order in {Elapsed}, average speed is {AvgWipeSpeed} sec/Mrow",
                                    count - _rows.Count, count, startedOn.Elapsed, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                            }
                        }
                    }
                    else
                    {
                        var lastRemoveableIndex = -1;
                        for (var i = 0; i < _rows.Count; i++)
                        {
                            if (_rows[i].State == RowState.Finished)
                            {
                                lastRemoveableIndex = i;
                                finishedCollection.Add(_rows[i]);
                            }
                            else if (_rows[i].State == RowState.Removed)
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
                            Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows while keeping order, average speed is {AvgWipeSpeed} sec/Mrow",
                                lastRemoveableIndex + 1, _rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                            _rows.RemoveRange(0, lastRemoveableIndex + 1);
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
                CounterCollection.IncrementCounter("wipes", 1, true);

                try
                {
                    var count = _rows.Count;

                    _rows = _rows.Where(x => x.State != RowState.Finished && x.State != RowState.Removed).ToList();
                    if (_rows.Count != count)
                    {
                        wipedRowCount += count - _rows.Count;
                        Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows, average speed is {AvgWipeSpeed} sec/Mrow",
                            count - _rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                    }
                }
                finally
                {
                    _rowsLock.ExitWriteLock();
                }
            }
        }

        private void CreateRowQueue()
        {
            _rowQueue = (IRowQueue)Activator.CreateInstance(Configuration.RowQueueType);
        }

        private bool PrepareOperations()
        {
            foreach (var op in Operations)
            {
                try
                {
                    PrepareOperation(op);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex, op);
                    return false;
                }
            }

            return true;
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
            foreach (var op in Operations)
            {
                try
                {
                    ShutdownOperation(op);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex, op);
                    return;
                }
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

        private void EnqueueOperation(IRowOperation operation, IRow row)
        {
            row.CurrentOperation = operation;
            _rowQueue.AddRow(row);
        }

        public void RemoveRow(IRow row, IRowOperation operation)
        {
            Context.SetRowOwner(row, null, operation);

            row.State = RowState.Removed;
            operation.CounterCollection.IncrementCounter("rows removed", 1, true);
            CounterCollection.IncrementCounter("rows removed by operations", 1, true);
        }

        public void RemoveRows(IEnumerable<IRow> rows, IRowOperation operation)
        {
            var n = 0;
            foreach (var row in rows)
            {
                Context.SetRowOwner(row, null, operation);

                row.State = RowState.Removed;
                n++;
            }

            operation.CounterCollection.IncrementCounter("rows removed", n, true);
            CounterCollection.IncrementCounter("rows removed by operations", n, true);
        }

        private void FlagRowAsFinished(IRow row)
        {
            Interlocked.Decrement(ref _activeRowCount);

            if (row.State == RowState.Normal)
            {
                row.State = RowState.Finished;
            }
        }

        private void LogOpCounters()
        {
            Context.Log(LogSeverity.Debug, this, "OPERATION COUNTERS");
            foreach (var op in Operations)
            {
                LogOpCounters(op);
            }
        }

        private void LogOpCounters(IRowOperation op)
        {
            var counters = op.CounterCollection.GetCounters();
            if (counters.Count == 0)
                return;

            foreach (var counter in counters)
            {
                Context.Log(LogSeverity.Debug, this, "({Operation}) {Counter} = {Value}", op.Name, counter.Name, counter.TypedValue);
            }

            if (op is IOperationGroup group)
            {
                foreach (var childOp in group.Then.Concat(group.Else))
                {
                    LogOpCounters(childOp);
                }
            }
        }

        public void Execute(IProcess caller = null)
        {
            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            Validate();

            if (Context.CancellationTokenSource.IsCancellationRequested)
                return;

            if (If?.Invoke(this) == false)
                return;

            try
            {
                ExecuteImpl();
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }
        }

        private void ExecuteImpl()
        {
            Context.Log(LogSeverity.Information, this, "operation host started");

            CreateRowQueue();
            try
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return;

                if (!PrepareOperations() || Context.CancellationTokenSource.IsCancellationRequested)
                    return;

                CreateWorker();
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    return;

                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);

                var swLoop = Stopwatch.StartNew();
                ReadingInput = true;
                var sourceRows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);
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

                    if (buffer.Count > Configuration.InputBufferSize || InputProcess.ConsumerShouldNotBuffer)
                    {
                        AddRows(buffer, null);
                        buffer.Clear();

                        Stopwatch swSleep = null;
                        while (true)
                        {
                            if (swLoop.ElapsedMilliseconds >= Configuration.MainLoopDelay)
                            {
                                Wipe(swProcessing, ref wipedRowCount);
                                swLoop.Restart();
                            }

                            if (Interlocked.Read(ref _activeRowCount) <= Configuration.ThrottlingLimit)
                                break;

                            if (swSleep != null)
                            {
                                if (swSleep.ElapsedMilliseconds >= Configuration.ThrottlingMaxSleep)
                                    break;
                            }
                            else
                            {
                                swSleep = Stopwatch.StartNew();
                            }

                            Thread.Sleep(Configuration.ThrottlingSleepResolution);
                        }

                        if (swSleep != null)
                        {
                            Context.Log(LogSeverity.Verbose, this, "slept {Elapsed} to lower active row count to {ActiveRowCount}, input buffer: {InputBufferCount}/{InputBufferSize}",
                                swSleep.Elapsed, Interlocked.Read(ref _activeRowCount), buffer.Count, Configuration.InputBufferSize);
                        }
                    }

                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        break;
                }

                if (buffer.Count > 0)
                {
                    AddRows(buffer, null);
                    buffer.Clear();
                }

                Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows in {Elapsed}", inputRowCount, LastInvocation.Elapsed);
                ReadingInput = false;

                var loopIndex = 0;
                while (true)
                {
                    Wipe(swProcessing, ref wipedRowCount);

                    if (TestDone())
                        break;

                    if (loopIndex > 100)
                    {
                        Thread.Sleep(Configuration.MainLoopDelay);
                    }
                    else
                    {
                        loopIndex++;
                        Thread.Sleep(10);
                    }
                }

                WaitForWorkerThread();
                ShutdownOperations();
            }
            finally
            {
                _rowQueue.Dispose();
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);

            LogCounters();
            LogOpCounters();
        }

        public Evaluator Evaluate(IProcess caller = null)
        {
            LastInvocation = Stopwatch.StartNew();
            Caller = caller;

            Validate();

            if (Context.CancellationTokenSource.IsCancellationRequested)
                return new Evaluator();

            if (If?.Invoke(this) == false)
                return new Evaluator();

            try
            {
                return new Evaluator(EvaluateImpl());
            }
            catch (EtlException ex) { Context.AddException(this, ex); }
            catch (Exception ex) { Context.AddException(this, new ProcessExecutionException(this, ex)); }

            return new Evaluator();
        }

        private IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "operation host started");

            var resultCount = 0;

            CreateRowQueue();
            try
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                if (!PrepareOperations() || Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                CreateWorker();
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    yield break;

                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);

                var swLoop = Stopwatch.StartNew();
                ReadingInput = true;
                var sourceRows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership(this);
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

                    if (buffer.Count >= Configuration.InputBufferSize || InputProcess.ConsumerShouldNotBuffer)
                    {
                        AddRows(buffer, null);
                        buffer.Clear();

                        while (true)
                        {
                            if (swLoop.ElapsedMilliseconds >= Configuration.MainLoopDelay)
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

                                    Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows of {OutputRowCount} in total, read input rows: {InputRowCount}, active rows: {ActiveRowCount}",
                                        finished.Count, resultCount, inputRowCount, Interlocked.Read(ref _activeRowCount));

                                    finished.Clear();
                                }
                            }

                            if (Interlocked.Read(ref _activeRowCount) <= Configuration.ThrottlingLimit)
                                break;

                            if (swSleep.IsRunning)
                            {
                                if (swSleep.ElapsedMilliseconds >= Configuration.ThrottlingMaxSleep)
                                    break;
                            }
                            else
                            {
                                swSleep.Restart();
                            }

                            Thread.Sleep(Configuration.ThrottlingSleepResolution);
                        }

                        if (swSleep.IsRunning)
                        {
                            Context.Log(LogSeverity.Verbose, this, "slept {Sleep} to lower active row count to {ActiveRowCount}", swSleep.Elapsed, Interlocked.Read(ref _activeRowCount));
                            swSleep.Stop();
                        }
                    }

                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        break;
                }

                if (buffer.Count > 0)
                {
                    AddRows(buffer, null);
                    buffer.Clear();
                }

                Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows in {Elapsed}", inputRowCount, LastInvocation.Elapsed);
                ReadingInput = false;

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

                        Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows of {OutputRowCount} in total, active rows: {ActiveRowCount}",
                            finished.Count, resultCount, Interlocked.Read(ref _activeRowCount));

                        finished.Clear();
                    }

                    if (TestDone())
                        break;

                    if (loopIndex > 100)
                    {
                        Thread.Sleep(Configuration.MainLoopDelay);
                    }
                    else
                    {
                        loopIndex++;
                        Thread.Sleep(10);
                    }
                }

                finished.AddRange(_rows.Where(x => x.State == RowState.Finished));
                if (finished.Count > 0)
                {
                    foreach (var finishedRow in finished)
                    {
                        yield return finishedRow;
                    }

                    Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows", _rows.Count);
                    Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows", finished.Count);
                    finished.Clear();
                }

                _rows.Clear();

                WaitForWorkerThread();
                ShutdownOperations();
            }
            finally
            {
                _rowQueue.Dispose();
            }

            Context.Log(LogSeverity.Debug, this, "finished and retuned {RowCount} rows of {AllRowCount} rows in {Elapsed}", resultCount, _rowsAdded, LastInvocation.Elapsed);

            LogCounters();
            LogOpCounters();
        }

        private void CreateWorker()
        {
            _workerThread = new Thread(tran =>
            {
                Transaction.Current = tran as Transaction;
                var rowsConsumer = _rowQueue.GetConsumer(_workerCancellationTokenSource.Token);
                var token = _workerCancellationTokenSource.Token;

                try
                {
                    foreach (var row in rowsConsumer)
                    {
                        if (token.IsCancellationRequested)
                            break;

                        if (!ProcessRow(row))
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                }

                Context.Log(LogSeverity.Verbose, this, "worker thread ended");
            });

            _workerThread.Start(Transaction.Current);
        }

        private bool ProcessRow(IRow row)
        {
            var operation = row.CurrentOperation;
            while (operation != null)
            {
                if (row.DeferState != DeferState.DeferDone)
                {
                    try
                    {
                        operation.Apply(row);
                    }
                    catch (OperationExecutionException) { throw; }
                    catch (Exception ex)
                    {
                        var exception = new OperationExecutionException(this, operation, row, "error raised during the execution of an operation", ex);
                        Context.AddException(this, exception, operation);
                        return false;
                    }

                    if (row.DeferState == DeferState.DeferWait)
                    {
                        EnqueueOperation(operation, row);
                        break;
                    }
                }

                if (row.DeferState == DeferState.DeferDone)
                {
                    row.DeferState = DeferState.None;
                }

                operation = GetNextOp(row);
                if (operation == null)
                    break;

                row.CurrentOperation = operation;
            }

            if (row.DeferState == DeferState.None)
            {
                FlagRowAsFinished(row);
            }

            return true;
        }

        public override void ValidateImpl()
        {
            if (Configuration == null)
                throw new ProcessParameterNullException(this, nameof(Configuration));

            if (Configuration.RowQueueType == null)
                throw new ProcessParameterNullException(this, nameof(Configuration.RowQueueType));

            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            foreach (var operation in Operations)
            {
                if (operation is IDeferredRowOperation defOp && Configuration.ThrottlingLimit < defOp.BatchSize * 10)
                {
                    Configuration.ThrottlingLimit = defOp.BatchSize * 10;
                    Context.Log(LogSeverity.Warning, this, nameof(Configuration) + "." + nameof(Configuration.ThrottlingLimit) + " must be >= than any deferred operation's " + nameof(IDeferredRowOperation.BatchSize) + " multiplied by 10 to prevent starving. The specified value is adjusted according to this recommendation.");
                }
            }
        }

        private void WaitForWorkerThread()
        {
            _workerThread.Join();
        }
    }
}