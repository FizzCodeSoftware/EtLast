namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
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

        /// <summary>
        /// Default true.
        /// </summary>
        public bool RemoveOriginalRow { get; set; } = true;

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
                rows = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().Select(x => x as IReadOnlySlimRow).ToList();
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

            if (!RemoveOriginalRow)
            {
                netTimeStopwatch.Stop();
                foreach (var row in rows)
                {
                    yield return row as IRow;
                }

                netTimeStopwatch.Start();
                resultCount += rows.Count;
            }

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
            Context.Log(LogSeverity.Debug, this, "processed {InputRowCount} rows and returned {RowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rows.Count, resultCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class InMemoryExplodeMutatorFluent
    {
        /// <summary>
        /// Create any number of new rows based on the input rows.
        /// <para>- memory footprint is high because all rows are collected before the delegate is called</para>
        /// <para>- if the rows can be exploded one-by-one without knowing the other rows, then using <see cref="ExplodeMutatorFluent.Explode(IFluentProcessMutatorBuilder, ExplodeMutator)"/> is highly recommended.</para>
        /// </summary>
        public static IFluentProcessMutatorBuilder ExplodeInMemory(this IFluentProcessMutatorBuilder builder, InMemoryExplodeMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}