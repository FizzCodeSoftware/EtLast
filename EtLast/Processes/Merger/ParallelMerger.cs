namespace FizzCode.EtLast;

public sealed class ParallelMerger(IEtlContext context) : AbstractMerger(context)
{
    protected override void ValidateImpl()
    {
    }

    protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
    {
        var threads = new List<Thread>();
        var finishedCount = 0;
        using var queue = new DefaultRowQueue();

        for (var i = 0; i < SequenceList.Count; i++)
        {
            var threadIndex = i; // capture variable for thread
            var sequence = SequenceList[threadIndex];

            var thread = new Thread(tran =>
            {
                var depTran = tran as DependentTransaction;
                try
                {
                    using (var ts = depTran != null ? new TransactionScope(depTran, TimeSpan.FromDays(1)) : null)
                    {
                        var rows = sequence.TakeRowsAndTransferOwnership(this);

                        foreach (var row in rows)
                        {
                            if (FlowState.IsTerminating)
                                break;

                            queue.AddRow(row);
                        }

                        ts?.Complete();

                        Interlocked.Increment(ref finishedCount);
                        if (finishedCount == SequenceList.Count)
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

        foreach (var row in queue.GetConsumer(Context.CancellationToken))
        {
            yield return row;
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ParallelMergerFluent
{
    public static IFluentSequenceMutatorBuilder ProcessOnMultipleThreads(this IFluentSequenceMutatorBuilder builder, int threadCount, Action<int, IFluentSequenceMutatorBuilder> mutatorBuilder)
    {
        var splitter = new Splitter<DefaultRowQueue>(builder.ProcessBuilder.Result.Context)
        {
            Name = "ParallelSplitter",
            InputProcess = builder.ProcessBuilder.Result,
        };

        var merger = new ParallelMerger(builder.ProcessBuilder.Result.Context)
        {
            Name = "ParallelMerger",
            SequenceList = [],
        };

        for (var i = 0; i < threadCount; i++)
        {
            var subBuilder = SequenceBuilder.Fluent;
            var subMutatorBuilder = subBuilder.ReadFrom(splitter);
            mutatorBuilder.Invoke(i, subMutatorBuilder);

            merger.SequenceList.Add(subBuilder.Result);
        }

        builder.ProcessBuilder.Result = merger;
        return builder;
    }
}
