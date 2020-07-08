namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    /// <summary>
    /// Input must be pre-grouped by key columns.
    /// Group key generation is applied on the input rows on-the-fly. The collected group is processed when a new key is found.
    /// - keeps all input rows in memory (!)
    /// </summary>
    public class SortedReduceGroupToSingleRowMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public Func<IReadOnlyRow, string> KeyGenerator { get; set; }
        public ReduceGroupToSingleRowDelegate Selector { get; set; }

        /// <summary>
        /// Default false. Setting to true means the Selector won't be called for groups with a single row - which can improve performance and/or introduce side effects.
        /// </summary>
        public bool IgnoreSelectorForSingleRowGroups { get; set; }

        public SortedReduceGroupToSingleRowMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (KeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(KeyGenerator));

            if (Selector == null)
                throw new ProcessParameterNullException(this, nameof(Selector));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var group = new List<IRow>();
            string lastKey = null;

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var success = true;

            var rowCount = 0;
            var resultRowCount = 0;

            while (!Context.CancellationTokenSource.IsCancellationRequested && success)
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;

                var row = enumerator.Current;
                rowCount++;
                var key = KeyGenerator.Invoke(row);
                if (key != lastKey)
                {
                    lastKey = key;

                    var exceptionCount = Context.ExceptionCount;
                    var groupRow = ReduceGroup(group);

                    if (groupRow != null)
                    {
                        resultRowCount++;
                        netTimeStopwatch.Stop();
                        yield return groupRow;
                        netTimeStopwatch.Start();
                    }

                    if (Context.ExceptionCount != exceptionCount)
                    {
                        success = false;
                        break;
                    }
                }

                group.Add(row);
            }

            if (success && group.Count > 0)
            {
                var groupRow = ReduceGroup(group);

                if (groupRow != null)
                {
                    resultRowCount++;
                    netTimeStopwatch.Stop();
                    yield return groupRow;
                    netTimeStopwatch.Start();
                }
            }

            netTimeStopwatch.Stop();

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rowCount, resultRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }

        private IRow ReduceGroup(List<IRow> group)
        {
            if (group.Count == 0)
                return null;

            IRow resultRow = null;
            if (group.Count != 1 || !IgnoreSelectorForSingleRowGroups)
            {
                try
                {
                    resultRow = Selector.Invoke(this, group);
                }
                catch (EtlException ex)
                {
                    Context.AddException(this, ex);
                    return null;
                }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, ex);
                    Context.AddException(this, exception);
                    return null;
                }

                foreach (var groupRow in group)
                {
                    if (groupRow != resultRow)
                        Context.SetRowOwner(groupRow, null);
                }
            }
            else
            {
                resultRow = group[0];
            }

            group.Clear();
            return resultRow;
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class SortedReduceGroupToSingleRowMutatorFluent
    {
        public static IFluentProcessMutatorBuilder AddSortedReduceGroupToSingleRowMutator(this IFluentProcessMutatorBuilder builder, SortedReduceGroupToSingleRowMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}