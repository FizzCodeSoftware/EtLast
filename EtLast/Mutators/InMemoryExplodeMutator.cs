namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public delegate IEnumerable<ISlimRow> InMemoryExplodeDelegate(InMemoryExplodeMutator proc, IReadOnlyList<IReadOnlySlimRow> rows);

    /// <summary>
    /// Useful only for small amount of data due to all input rows are collected into a List and processed at once.
    /// </summary>
    public class InMemoryExplodeMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public InMemoryExplodeDelegate Action { get; set; }

        public InMemoryExplodeMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            List<IReadOnlySlimRow> rows;
            try
            {
                rows = InputProcess.Evaluate(this).TakeRowsAndReleaseOwnership().Select(x => x as IReadOnlySlimRow).ToList();
            }
            catch (Exception ex)
            {
                Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                yield break;
            }

            var resultCount = 0;

            netTimeStopwatch.Stop();
            var enumerator = Action.Invoke(this, rows).GetEnumerator();
            netTimeStopwatch.Start();

            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                ISlimRow newRow;
                try
                {
                    if (!enumerator.MoveNext())
                        break;

                    newRow = enumerator.Current;
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ProcessExecutionException.Wrap(this, ex));
                    break;
                }

                resultCount++;
                netTimeStopwatch.Stop();
                yield return Context.CreateRow(this, newRow);
                netTimeStopwatch.Start();
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "processed {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
                resultCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }

        public IEnumerator<IMutator> GetEnumerator()
        {
            yield return this;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            yield return this;
        }
    }
}