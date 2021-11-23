namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractMutator : AbstractEvaluable, IMutator
    {
        public IProducer InputProcess { get; set; }
        public RowTestDelegate If { get; set; }
        public RowTagTestDelegate TagFilter { get; set; }

        protected AbstractMutator(IEtlContext context)
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

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var mutatedRowCount = 0;
            var ignoredRowCount = 0;

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

                var kept = false;
                try
                {
                    foreach (var mutatedRow in MutateRow(row))
                    {
                        if (mutatedRow == row)
                            kept = true;

                        if (mutatedRow.CurrentProcess != this)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                            break;
                        }

                        mutatedRows.Add(mutatedRow);
                    }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ProcessExecutionException.Wrap(this, row, ex));
                    break;
                }

                if (!kept)
                {
                    Context.SetRowOwner(row, null);
                }

                netTimeStopwatch.Stop();
                foreach (var mutatedRow in mutatedRows)
                {
                    yield return mutatedRow;
                }

                netTimeStopwatch.Start();

                mutatedRows.Clear();
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
                Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount} of {TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                    mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
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

        protected abstract IEnumerable<IRow> MutateRow(IRow row);

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