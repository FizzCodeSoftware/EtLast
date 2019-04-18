﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class OperationProcess : AbstractOperationProcess
    {
        public OperationProcessConfiguration Configuration { get; set; } = new OperationProcessConfiguration();

        private readonly List<Thread> _workerThreads = new List<Thread>();

        public OperationProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;

            if (Configuration == null) throw new InvalidProcessParameterException(this, nameof(Configuration), Configuration, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Configuration.WorkerCount <= 0) throw new InvalidProcessParameterException(this, nameof(Configuration.WorkerCount), Configuration.WorkerCount, "value must be greater than 0");
            if (Configuration.WorkerType == null) throw new InvalidProcessParameterException(this, nameof(Configuration.WorkerType), Configuration.WorkerType, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Configuration.RowQueueType == null) throw new InvalidProcessParameterException(this, nameof(Configuration.RowQueueType), Configuration.RowQueueType, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (InputProcess == null) throw new InvalidProcessParameterException(this, nameof(InputProcess), InputProcess, InvalidOperationParameterException.ValueCannotBeNullMessage);

            Context.Log(LogSeverity.Debug, this, "started using worker: {WorkerCount} of {WorkerType}, queue: {RowQueueType}, order: {KeepOrder}, input buffer: {InputBufferSize}, loop delay: {MainLoopDelay}", Configuration.WorkerCount, Configuration.WorkerType.Name, Configuration.RowQueueType.Name, Configuration.KeepOrder ? "keep" : "ignore", Configuration.InputBufferSize, Configuration.MainLoopDelay);
            var sw = Stopwatch.StartNew();

            CreateRowQueue(Configuration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) yield break;

            CreateWorkerThreads();
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
                            WipeAndGet(finished, swProcessing, Configuration.KeepOrder, ref wipedRowCount);
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
                Thread.Sleep(Configuration.MainLoopDelay);

                WipeAndGet(finished, swProcessing, Configuration.KeepOrder, ref wipedRowCount);

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

            WaitForWorkerThreads();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished and retuned {RowCount} rows of {AllRowCount} rows in {Elapsed}", resultCount, RowsAdded, sw.Elapsed);
        }

        public override void EvaluateWithoutResult(IProcess caller = null)
        {
            Caller = caller;

            if (Configuration == null) throw new InvalidProcessParameterException(this, nameof(Configuration), Configuration, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Configuration.WorkerCount <= 0) throw new InvalidProcessParameterException(this, nameof(Configuration.WorkerCount), Configuration.WorkerCount, "value must be greater than 0");
            if (Configuration.WorkerType == null) throw new InvalidProcessParameterException(this, nameof(Configuration.WorkerType), Configuration.WorkerType, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Configuration.RowQueueType == null) throw new InvalidProcessParameterException(this, nameof(Configuration.RowQueueType), Configuration.RowQueueType, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (InputProcess == null) throw new InvalidProcessParameterException(this, nameof(InputProcess), InputProcess, InvalidOperationParameterException.ValueCannotBeNullMessage);

            Context.Log(LogSeverity.Debug, this, "settings: worker count: {WorkerCount} of {WorkerType}, queue: {RowQueueType}, order: {KeepOrder}, input buffer: {InputBufferSize}, loop delay: {MainLoopDelay}", Configuration.WorkerCount, Configuration.WorkerType.Name, Configuration.RowQueueType.Name, Configuration.KeepOrder ? "keep" : "ignore", Configuration.InputBufferSize, Configuration.MainLoopDelay);

            var sw = Stopwatch.StartNew();

            CreateRowQueue(Configuration.RowQueueType);
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            PrepareOperations();
            if (Context.CancellationTokenSource.IsCancellationRequested) return;

            CreateWorkerThreads();
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
                Thread.Sleep(Configuration.MainLoopDelay);

                Wipe(swProcessing, ref wipedRowCount);

                if (TestDone()) break;
            }

            WaitForWorkerThreads();
            ShutdownOperations();
            LogStats();

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", sw.Elapsed);
        }

        private void WaitForWorkerThreads()
        {
            foreach (var thread in _workerThreads)
            {
                thread.Join();
            }
        }

        private void CreateWorkerThreads()
        {
            for (int i = 1; i <= Configuration.WorkerCount; i++)
            {
                var worker = (IOperationProcessWorker)Activator.CreateInstance(Configuration.WorkerType);
                CreateWorkerThread(worker);
            }
        }

        private void CreateWorkerThread(IOperationProcessWorker worker)
        {
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