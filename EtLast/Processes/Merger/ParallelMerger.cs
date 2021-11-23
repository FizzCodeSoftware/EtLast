namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public sealed class ParallelMerger : AbstractMerger
    {
        public ParallelMerger(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var threads = new List<Thread>();
            var finishedCount = 0;
            using var queue = new DefaultRowQueue();

            for (var i = 0; i < ProcessList.Count; i++)
            {
                var threadIndex = i; // capture variable for thread
                var inputProcess = ProcessList[threadIndex];

                var thread = new Thread(tran =>
                {
                    var depTran = tran as DependentTransaction;
                    try
                    {
                        using (var ts = depTran != null ? new TransactionScope(depTran, TimeSpan.FromDays(1)) : null)
                        {
                            var rows = inputProcess.Evaluate(this).TakeRowsAndTransferOwnership();

                            foreach (var row in rows)
                            {
                                queue.AddRow(row);
                            }

                            ts?.Complete();

                            Interlocked.Increment(ref finishedCount);
                            if (finishedCount == ProcessList.Count)
                            {
                                queue.SignalNoMoreRows();
                            }
                        }
                    }
                    finally
                    {
                        if (depTran != null)
                        {
                            depTran.Complete();
                            depTran.Dispose();
                        }
                    }
                });

                var dependentTransaction = Transaction.Current?.DependentClone(DependentCloneOption.BlockCommitUntilComplete);
                thread.Start(dependentTransaction);
                threads.Add(thread);
            }

            foreach (var row in queue.GetConsumer(Context.CancellationTokenSource.Token))
            {
                yield return row;
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", InvocationInfo.LastInvocationStarted.Elapsed);
            Context.RegisterProcessInvocationEnd(this);
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ParallelMergerFluent
    {
        public static IFluentProcessMutatorBuilder ProcessOnMultipleThreads(this IFluentProcessMutatorBuilder builder, int threadCount, Action<int, IFluentProcessMutatorBuilder> mutatorBuilder)
        {
            var splitter = new Splitter<DefaultRowQueue>(builder.ProcessBuilder.Result.Context)
            {
                Name = "ParallelSplitter",
                InputProcess = builder.ProcessBuilder.Result,
            };

            var merger = new ParallelMerger(builder.ProcessBuilder.Result.Context)
            {
                Name = "ParallelMerger",
                ProcessList = new List<IProducer>(),
            };

            for (var i = 0; i < threadCount; i++)
            {
                var subBuilder = ProcessBuilder.Fluent;
                var subMutatorBuilder = subBuilder.ReadFrom(splitter);
                mutatorBuilder.Invoke(i, subMutatorBuilder);

                var subProcess = subBuilder.Result;
                merger.ProcessList.Add(subProcess);
            }

            builder.ProcessBuilder.Result = merger;
            return builder;
        }
    }
}