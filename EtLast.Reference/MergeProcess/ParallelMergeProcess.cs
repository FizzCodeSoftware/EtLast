namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class ParallelMergeProcess : AbstractMergeProcess
    {
        public ParallelMergeProcess(IEtlContext context, IRowSetMerger merger, string name = null)
            : base(context, merger, name)
        {
        }

        public override void Validate()
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "started");

            var threads = new List<Thread>();
            var resultSets = new ConcurrentBag<IEnumerable<IRow>>();

            foreach (var inputProcess in ProcessList)
            {
                var thread = new Thread(tran =>
                {
                    var depTran = tran as DependentTransaction;
                    try
                    {
                        using (var ts = depTran != null ? new TransactionScope(depTran, TimeSpan.FromDays(1)) : null)
                        {
                            var rows = inputProcess.Evaluate(this);
                            resultSets.Add(rows);

                            ts?.Complete();
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

            foreach (var thread in threads)
            {
                thread.Join();
            }

            var result = Merger.Merge(resultSets.ToList());

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", LastInvocation.Elapsed);
            return result;
        }
    }
}