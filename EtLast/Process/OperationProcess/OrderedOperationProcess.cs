namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class OrderedOperationProcess : AbstractOperationProcess
    {
        public OrderedOperationProcessConfiguration Configuration { get; set; } = new OrderedOperationProcessConfiguration();

        private Thread _workerThread;

        public OrderedOperationProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            if (Configuration == null) throw new ProcessParameterNullException(this, nameof(Configuration));
            if (Configuration.RowQueueType == null) throw new ProcessParameterNullException(this, nameof(Configuration.RowQueueType));
            if (InputProcess == null) throw new ProcessParameterNullException(this, nameof(InputProcess));

            Context.Log(LogSeverity.Debug, this, "started using queue: {RowQueueType}, input buffer: {InputBufferSize}, loop delay: {MainLoopDelay}", Configuration.RowQueueType.Name, Configuration.InputBufferSize, Configuration.MainLoopDelay);
            var sw = Stopwatch.StartNew();

            CreateRowQueue(Configuration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            CreateWorkerThread();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            var finished = new List<IRow>();
            var resultCount = 0;

            Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

            var swLoop = Stopwatch.StartNew();
            var sourceRows = InputProcess.Evaluate(this);
            var buffer = new List<IRow>();
            var inputRowCount = 0;
            var wipedRowCount = 0;

            var swProcessing = Stopwatch.StartNew();
            var swSleep = new Stopwatch();

            foreach (var row in sourceRows)
            {
                row.CurrentOperation = null;
                row.State = RowState.Normal;

                inputRowCount++;
                buffer.Add(row);

                if (buffer.Count >= Configuration.InputBufferSize)
                {
                    AddRows(buffer, null);
                    buffer.Clear();

                    while (true)
                    {
                        if (swLoop.ElapsedMilliseconds >= Configuration.MainLoopDelay)
                        {
                            WipeAndGet(finished, swProcessing, true, ref wipedRowCount);
                            swLoop.Restart();

                            if (finished.Count > 0)
                            {
                                resultCount += finished.Count;
                                foreach (var finishedRow in finished)
                                {
                                    yield return finishedRow;
                                }

                                Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows of {OutputRowCount} in total, read input rows: {InputRowCount}, active rows: {ActiveRowCount}",
                                    finished.Count, resultCount, inputRowCount, ActiveRowCount);

                                finished.Clear();
                            }
                        }

                        if (ActiveRowCount <= Configuration.ThrottlingLimit) break;
                        if (swSleep.IsRunning)
                        {
                            if (swSleep.ElapsedMilliseconds >= Configuration.ThrottlingMaxSleep) break;
                        }
                        else swSleep.Restart();

                        Thread.Sleep(Configuration.ThrottlingSleepResolution);
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

            while (true)
            {
                WipeAndGet(finished, swProcessing, true, ref wipedRowCount);

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

                Thread.Sleep(Configuration.MainLoopDelay);
            }

            // safely ignore Configuration.KeepOrder because _rows is already ordered
            // so all remaining (finished) items are ordered
            // todo: test because this is optimistic concurrency
            finished.AddRange(Rows.Where(x => x.State == RowState.Finished));
            if (finished.Count > 0)
            {
                resultCount += finished.Count;
                foreach (var finishedRow in finished)
                {
                    yield return finishedRow;
                }

                Context.Log(LogSeverity.Verbose, this, "wiped {RowCount} rows", Rows.Count);
                Context.Log(LogSeverity.Debug, this, "returned {RowCount} rows", finished.Count);
                finished.Clear();
            }

            Rows.Clear();

            WaitForWorkerThread();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished and retuned {RowCount} rows of {AllRowCount} rows in {Elapsed}", resultCount, RowsAdded, sw.Elapsed);
        }

        public override void EvaluateWithoutResult(IProcess caller = null)
        {
            Caller = caller;

            if (Configuration == null) throw new ProcessParameterNullException(this, nameof(Configuration));
            if (Configuration.RowQueueType == null) throw new ProcessParameterNullException(this, nameof(Configuration.RowQueueType));
            if (InputProcess == null) throw new ProcessParameterNullException(this, nameof(InputProcess));

            Context.Log(LogSeverity.Debug, this, "started using queue: {RowQueueType}, input buffer: {InputBufferSize}, loop delay: {MainLoopDelay}", Configuration.RowQueueType.Name, Configuration.InputBufferSize, Configuration.MainLoopDelay);

            var sw = Stopwatch.StartNew();

            CreateRowQueue(Configuration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            CreateWorkerThread();
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

                if (buffer.Count > Configuration.InputBufferSize)
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

                        if (ActiveRowCount <= Configuration.ThrottlingLimit) break;
                        if (swSleep != null)
                        {
                            if (swSleep.ElapsedMilliseconds >= Configuration.ThrottlingMaxSleep) break;
                        }
                        else swSleep = Stopwatch.StartNew();

                        Thread.Sleep(Configuration.ThrottlingSleepResolution);
                    }

                    if (swSleep != null)
                    {
                        Context.Log(LogSeverity.Verbose, this, "slept {Elapsed} to lower active row count to {ActiveRowCount}, input buffer: {InputBufferCount}/{InputBufferSize}", swSleep.Elapsed, ActiveRowCount, buffer.Count, Configuration.InputBufferSize);
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

            while (true)
            {
                Wipe(swProcessing, ref wipedRowCount);

                if (TestDone()) break;

                Thread.Sleep(Configuration.MainLoopDelay);
            }

            WaitForWorkerThread();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", sw.Elapsed);
        }

        private void WaitForWorkerThread()
        {
            _workerThread.Join();
        }

        private void CreateWorkerThread()
        {
            _workerThread = new Thread(tran =>
            {
                Transaction.Current = tran as Transaction;
                var rowsConsumer = RowQueue.GetConsumer(WorkerCancellationTokenSource.Token);

                try
                {
                    foreach (var row in rowsConsumer)
                    {
                        if (WorkerCancellationTokenSource.Token.IsCancellationRequested)
                        {
                            break;
                        }

                        var operation = row.CurrentOperation;
                        while (operation != null)
                        {
                            try
                            {
                                operation.Apply(row);
                            }
                            catch (OperationExecutionException) { throw; }
                            catch (Exception ex)
                            {
                                var exception = new OperationExecutionException(this, operation, row, "error raised during the execution of an operation", ex);
                                throw exception;
                            }

                            operation = GetNextOp(row);
                            if (operation == null) break;

                            row.CurrentOperation = operation;
                        }

                        FlagRowAsFinished(row);
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                }

                Context.Log(LogSeverity.Debug, this, "worker thread ended");
            });

            _workerThread.Start(Transaction.Current);
        }
    }
}