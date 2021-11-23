namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractBatchedMutator : AbstractEvaluable, IMutator
    {
        public IProducer InputProcess { get; set; }
        public RowTestDelegate If { get; init; }
        public RowTagTestDelegate TagFilter { get; set; }

        public abstract int BatchSize { get; init; }

        /// <summary>
        /// Default false.
        /// </summary>
        protected bool UseBatchKeys { get; init; }

        protected AbstractBatchedMutator(IEtlContext context)
            : base(context)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            try
            {
                StartMutator();
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                yield break;
            }

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
                if (If != null)
                {
                    try
                    {
                        apply = If.Invoke(row);
                    }
                    catch (Exception ex)
                    {
                        Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
                        break;
                    }

                    if (!apply)
                    {
                        ignoredRowCount++;
                        netTimeStopwatch.Stop();
                        yield return row;
                        netTimeStopwatch.Start();
                        continue;
                    }
                }

                if (TagFilter != null)
                {
                    try
                    {
                        apply = TagFilter.Invoke(row.Tag);
                    }
                    catch (Exception ex)
                    {
                        Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
                        break;
                    }

                    if (!apply)
                    {
                        ignoredRowCount++;
                        netTimeStopwatch.Stop();
                        yield return row;
                        netTimeStopwatch.Start();
                        continue;
                    }
                }

                mutatedRowCount++;

                bool mutationHappened, removeOriginal;
                try
                {
                    MutateSingleRow(row, mutatedRows, out removeOriginal, out mutationHappened);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
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
                        try
                        {
                            var key = GetBatchKey(row);
                            batchKeys.Add(key);
                        }
                        catch (Exception ex)
                        {
                            Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
                            failed = true;
                            break;
                        }
                    }

                    if ((UseBatchKeys && batchKeys.Count >= BatchSize) || (!UseBatchKeys && batch.Count >= BatchSize))
                    {
                        batchCount++;
                        try
                        {
                            MutateBatch(batch, mutatedRows, removedRows);
                        }
                        catch (Exception ex)
                        {
                            Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
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
                try
                {
                    MutateBatch(batch, mutatedRows, removedRows);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                    failed = true;
                }

                if (!failed)
                {
                    foreach (var removedRow in removedRows)
                    {
                        Context.SetRowOwner(removedRow, null);
                    }

                    netTimeStopwatch.Stop();
                    foreach (var mutatedRow in mutatedRows)
                    {
                        if (mutatedRow.CurrentProcess != this)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                            failed = true;
                            break;
                        }

                        yield return mutatedRow;
                    }

                    netTimeStopwatch.Start();
                }

                mutatedRows.Clear();
                removedRows.Clear();
                batch.Clear();
                batchKeys.Clear();
            }

            try
            {
                CloseMutator();
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                netTimeStopwatch.Stop();
                Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
                yield break;
            }

            netTimeStopwatch.Stop();

            if (mutatedRowCount + ignoredRowCount > 0)
            {
                Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount}/{TotalRowCount} rows in {Elapsed}/{ElapsedWallClock} in {BatchCount} batches",
                    mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed, batchCount);
            }

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

        protected abstract void MutateSingleRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed);

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