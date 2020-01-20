namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class ParallelMergeProcess : AbstractMergeProcess
    {
        public ParallelMergeProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void ValidateImpl()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "started");

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
                            var rows = inputProcess.Evaluate(this);

                            foreach (var row in rows)
                            {
                                Context.SetRowOwner(row, this);
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

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);
        }
    }
}