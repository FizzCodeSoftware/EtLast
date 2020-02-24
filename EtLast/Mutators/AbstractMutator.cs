namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class AbstractMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        protected AbstractMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            StartMutator();

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
                try
                {
                    apply = If?.Invoke(row) != false;
                }
                catch (ProcessExecutionException ex)
                {
                    Context.AddException(this, ex);
                    break;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, row, ex));
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

                var kept = false;
                try
                {
                    foreach (var mutatedRow in MutateRow(row))
                    {
                        if (mutatedRow == row)
                            kept = true;

                        if (mutatedRow.HasStaging)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "unfinished staging"));
                            break;
                        }

                        if (mutatedRow.CurrentProcess != this)
                        {
                            Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                            break;
                        }

                        mutatedRows.Add(mutatedRow);
                    }
                }
                catch (ProcessExecutionException ex)
                {
                    Context.AddException(this, ex);
                    break;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, new ProcessExecutionException(this, row, ex));
                    break;
                }

                if (!kept)
                {
                    Context.SetRowOwner(row, null);
                }

                netTimeStopwatch.Stop();
                foreach (var mutatedRow in mutatedRows)
                {
                    if (mutatedRow.HasStaging)
                    {
                        Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "unfinished staging"));
                        break;
                    }

                    if (mutatedRow.CurrentProcess != this)
                    {
                        Context.AddException(this, new ProcessExecutionException(this, mutatedRow, "mutator returned a row without proper ownership"));
                        break;
                    }

                    yield return mutatedRow;
                }

                netTimeStopwatch.Start();

                mutatedRows.Clear();
            }

            CloseMutator();
            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "mutated {MutatedRowCount}/{TotalRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                mutatedRowCount, mutatedRowCount + ignoredRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

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