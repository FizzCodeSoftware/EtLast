namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    public abstract class AbstractOperationProcess : IOperationProcess
    {
        private List<IRow> _rows = new List<IRow>();
        protected List<IRow> Rows => _rows;

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
        public IReadOnlyList<IRowOperation> Operations => _operations;
        public IProcess InputProcess { get; set; }

        protected AbstractOperationProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new InvalidProcessParameterException(this, nameof(context), context, InvalidOperationParameterException.ValueCannotBeNullMessage);
            Name = name ?? nameof(OperationProcess);
        }

        public T AddOperation<T>(T operation)
            where T : IRowOperation
        {
            operation.SetParent(this, Operations.Count);

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
                    _rows.Add(row);
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

            if (operation != null) operation.Stat.IncrementCounter("rows added", 1);
        }

        public void AddRows(ICollection<IRow> rows, IRowOperation operation)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    _rows.AddRange(rows);
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
                if (row.CurrentOperation.Index < Operations.Count - 1)
                {
                    nextOp = Operations[row.CurrentOperation.Index + 1];
                }
            }
            else
            {
                nextOp = Operations.FirstOrDefault();
            }

            return nextOp;
        }

        protected bool TestDone()
        {
            var done = Interlocked.Read(ref _activeRowCount) == 0;
            var terminated = Context.CancellationTokenSource.IsCancellationRequested;
            if (done || terminated)
            {
                WorkerCancellationTokenSource.Cancel();
                return true;
            }

            return false;
        }

        protected void WipeAndGet(List<IRow> finishedCollection, Stopwatch swProcessing, bool keepOrder, ref int wipedRowCount)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    if (!keepOrder)
                    {
                        var sw = Stopwatch.StartNew();
                        var hs = new HashSet<IRow>(_rows.Where(x => x.State == RowState.Finished));
                        if (hs.Count > 0)
                        {
                            finishedCollection.AddRange(hs);

                            var count = _rows.Count;
                            _rows = _rows.Where(x => !hs.Contains(x) && x.State != RowState.Removed).ToList();
                            if (_rows.Count != count)
                            {
                                wipedRowCount += count - _rows.Count;
                                Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows without keeping order in {Elapsed}, average speed is {AvgWipeSpeed} msec/Krow", count - _rows.Count, count, sw.Elapsed, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                            }
                        }
                    }
                    else
                    {
                        var lastRemoveableIndex = -1;
                        for (int i = 0; i < _rows.Count; i++)
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
                            else break;
                        }

                        if (lastRemoveableIndex > -1)
                        {
                            Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} of {AllRowCount} rows while keeping order, average speed is {AvgWipeSpeed} msec/Krow", lastRemoveableIndex + 1, _rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
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

        protected void Wipe(Stopwatch swProcessing, ref int wipedRowCount)
        {
            if (_rowsLock.TryEnterWriteLock(10000))
            {
                try
                {
                    var count = _rows.Count;
                    _rows = _rows.Where(x => x.State == RowState.Normal).ToList();
                    if (_rows.Count != count)
                    {
                        wipedRowCount += count - _rows.Count;
                        Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows, average speed is {AvgWipeSpeed} msec/Krow", count - _rows.Count, Math.Round(swProcessing.ElapsedMilliseconds * 1000 / (double)wipedRowCount, 1));
                    }
                }
                finally
                {
                    _rowsLock.ExitWriteLock();
                }
            }
        }

        protected void CreateRowQueue(Type type)
        {
            RowQueue = (IRowQueue)Activator.CreateInstance(type);
        }

        protected void PrepareOperations()
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

        protected void PrepareOperation(IRowOperation op)
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

        protected void ShutdownOperations()
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

        protected void ShutdownOperation(IRowOperation op)
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

        protected void LogStats()
        {
            var sb = new System.Text.StringBuilder();
            foreach (var op in Operations)
            {
                LogOpStat(op, sb);
            }
        }

        protected void LogOpStat(IRowOperation op, System.Text.StringBuilder sb)
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


        public abstract void EvaluateWithoutResult(IProcess caller = null);
        public abstract IEnumerable<IRow> Evaluate(IProcess caller = null);
    }
}