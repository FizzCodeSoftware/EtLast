namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class ParallelMerger : AbstractMerger
    {
        public ParallelMerger(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var threads = new List<Thread>();
            var finished = new bool[ProcessList.Count];
            using var queue = new DefaultRowQueue();

            for (var i = 0; i < ProcessList.Count; i++)
            {
                var inputProcess = ProcessList[i];

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

                            finished[i] = true;
                            if (finished.All(x => x))
                                queue.SignalNoMoreRows();
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
}