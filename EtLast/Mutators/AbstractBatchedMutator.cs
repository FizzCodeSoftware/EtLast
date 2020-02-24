namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractBatchedMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        public abstract int BatchSize { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        protected bool UseBatchKeys { get; set; }

        protected AbstractBatchedMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            StartMutator();

            var mutatedRows = new List<IRow>();
            var removedRows = new List<IRow>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var batch = new List<IRow>();
            var batchKeys = new HashSet<string>();

            var failed = false;
            var mutatedRowCount = 0;
            var ignoredRowCount = 0;
            var batchCount = 0;

            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;

                var row = enumerator.Current;
                var apply = false;
                try
                {
                    apply = If?.Invoke(row) != false;
                }
                catch (ProcessExecutionException ex)
                {
                    Context.AddException(this, ex);
                    failed = true;
                    break;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, row, ex));
                    failed = true;
                    break;
                }

                if (!apply)
                {
                    ignoredRowCount++;
                    CounterCollection.IncrementCounter("ignored", 1, true);
                    netTimeStopwatch.Stop();
                    yield return row;
                    netTimeStopwatch.Start();
                    continue;
                }

                mutatedRowCount++;
                CounterCollection.IncrementCounter("mutated", 1, true);

                bool mutationHappened, removeOriginal;
                try
                {
                    MutateRow(row, mutatedRows, out removeOriginal, out mutationHappened);
                }
                catch (ProcessExecutionException ex)
                {
                    Context.AddException(this, ex);
                    failed = true;
                    break;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, row, ex));
                    failed = true;
                    break;
                }

                if (mutationHappened)
                {
                    if (removeOriginal)
                    {
                        Context.SetRowOwner(row, null);
                    }

                    netTimeStopwatch.Stop();

                    foreach (var mutatedRow in mutatedRows)
                    {
                        if (mutatedRow.HasStaging)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "unfinished staging"));
                            failed = true;
                            break;
                        }

                        if (mutatedRow.CurrentProcess != this)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                            failed = true;
                            break;
                        }

                        netTimeStopwatch.Stop();
                        yield return mutatedRow;
                        netTimeStopwatch.Start();
                    }

                    mutatedRows.Clear();
                }
                else
                {
                    batch.Add(row);

                    if (UseBatchKeys)
                    {
                        var key = GetBatchKey(row);
                        batchKeys.Add(key);
                    }

                    if ((UseBatchKeys && batchKeys.Count >= BatchSize) || (!UseBatchKeys && batch.Count >= BatchSize))
                    {
                        batchCount++;
                        CounterCollection.IncrementCounter("batches", 1, true);
                        try
                        {
                            MutateBatch(batch, mutatedRows, removedRows);
                        }
                        catch (ProcessExecutionException ex)
                        {
                            Context.AddException(this, ex);
                            failed = true;
                            break;
                        }
                        catch (Exception ex)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, row, ex));
                            failed = true;
                            break;
                        }

                        foreach (var removedRow in removedRows)
                        {
                            Context.SetRowOwner(removedRow, null);
                        }

                        netTimeStopwatch.Stop();
                        foreach (var mutatedRow in mutatedRows)
                        {
                            if (mutatedRow.HasStaging)
                            {
                                Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "unfinished staging"));
                                failed = true;
                                break;
                            }

                            if (mutatedRow.CurrentProcess != this)
                            {
                                Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                                failed = true;
                                break;
                            }

                            yield return mutatedRow;
                        }

                        netTimeStopwatch.Start();

                        mutatedRows.Clear();
                        removedRows.Clear();
                        batch.Clear();
                        batchKeys.Clear();
                    }
                }
            }

            // process remaining rows
            if (batch.Count > 0 && !failed)
            {
                batchCount++;
                CounterCollection.IncrementCounter("batches", 1, true);
                try
                {
                    MutateBatch(batch, mutatedRows, removedRows);
                }
                catch (ProcessExecutionException ex)
                {
                    Context.AddException(this, ex);
                    failed = true;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, ex));
                    failed = true;
                }

                if (!failed)
                {
                    foreach (var removedRow in removedRows)
                    {
                        Context.SetRowOwner(removedRow, null);
                    }

                    foreach (var mutatedRow in mutatedRows)
                    {
                        netTimeStopwatch.Stop();
                        yield return mutatedRow;
                        netTimeStopwatch.Start();
                    }
                }

                mutatedRows.Clear();
                removedRows.Clear();
                batch.Clear();
                batchKeys.Clear();
            }

            CloseMutator();
            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount}/{TotalRowCount} rows in {Elapsed}/{ElapsedWallClock} in {BatchCount} batches",
                mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed, batchCount);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }

        protected sealed override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            ValidateMutator();
        }

        protected virtual void ValidateMutator()
        {
        }

        protected virtual void StartMutator()
        {
        }

        protected virtual void CloseMutator()
        {
        }

        protected abstract void MutateRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed);

        protected abstract void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows);

        protected virtual string GetBatchKey(IRow row)
        {
            return null;
        }

        public IEnumerable<IMutator> GetMutators()
        {
            yield return this;
        }

        public IEnumerator<IMutator> GetEnumerator()
        {
            yield return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            yield return this;
        }
    }
}