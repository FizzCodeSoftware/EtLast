namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;

    public delegate IRow ReduceGroupToSingleRowDelegate(IProcess process, IReadOnlyList<IRow> groupRows);

    /// <summary>
    /// Input can be unordered. Group key generation is applied on the input rows on-the-fly, but group processing is started only after all groups are created.
    /// - keeps all input rows in memory (!)
    /// </summary>
    public class ReduceGroupToSingleRowMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public Func<IReadOnlyRow, string> KeyGenerator { get; init; }
        public ReduceGroupToSingleRowDelegate Selector { get; init; }

        /// <summary>
        /// Default false. Setting to true means the Selector won't be called for groups with a single row - which can improve performance and/or introduce side effects.
        /// </summary>
        public bool IgnoreSelectorForSingleRowGroups { get; init; }

        public ReduceGroupToSingleRowMutator(ITopic topic, string name)
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
            var groups = new Dictionary<string, object>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var rowCount = 0;
            while (!Context.CancellationTokenSource.IsCancellationRequested)
            {
                netTimeStopwatch.Stop();
                var finished = !enumerator.MoveNext();
                netTimeStopwatch.Start();
                if (finished)
                    break;

                var row = enumerator.Current;
                rowCount++;
                var key = KeyGenerator.Invoke(row);
                if (!groups.TryGetValue(key, out var group))
                {
                    groups.Add(key, row);
                }
                else
                {
                    if (group is not List<IRow> list)
                    {
                        groups[key] = list = new List<IRow>();
                        list.Add(group as IRow);
                    }

                    list.Add(row);
                }
            }

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and created {GroupCount} groups in {Elapsed}/{ElapsedWallClock}",
                rowCount, groups.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            var fakeList = new List<IRow>();

            var resultRowCount = 0;
            foreach (var group in groups.Values)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                var singleRow = group as IRow;

                if (IgnoreSelectorForSingleRowGroups && singleRow != null)
                {
                    resultRowCount++;

                    netTimeStopwatch.Stop();
                    yield return singleRow;
                    netTimeStopwatch.Start();

                    continue;
                }

                IRow resultRow = null;

                List<IRow> list = null;
                if (singleRow != null)
                {
                    fakeList.Clear();
                    fakeList.Add(singleRow);
                    list = fakeList;
                }
                else
                {
                    list = group as List<IRow>;
                }

                try
                {
                    resultRow = Selector.Invoke(this, list);
                }
                catch (EtlException ex)
                {
                    Context.AddException(this, ex);
                    break;
                }
                catch (Exception ex)
                {
                    var exception = new ProcessExecutionException(this, ex);
                    Context.AddException(this, exception);
                    break;
                }

                foreach (var row in list)
                {
                    if (row != resultRow)
                        Context.SetRowOwner(row, null);
                }

                if (resultRow != null)
                {
                    resultRowCount++;

                    netTimeStopwatch.Stop();
                    yield return resultRow;
                    netTimeStopwatch.Start();
                }
            }

            netTimeStopwatch.Stop();

            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rowCount, resultRowCount, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
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
    public static class ReduceGroupToSingleRowMutatorFluent
    {
        /// <summary>
        /// Organize input rows into groups and activates a selector which must select zero or one row from the group to be kept. All other rows of the group are discared.
        /// <para>- input can be unordered</para>
        /// <para>- returns all selected rows at once when everything is processed. Memory footprint is high because all rows in all groups are collected before selection</para>
        /// <para>- if the input is ordered then <see cref="SortedReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRowOrdered(IFluentProcessMutatorBuilder, SortedReduceGroupToSingleRowMutator)"/> should be used for much lower memory footprint and stream-like behavior</para>
        /// </summary>
        public static IFluentProcessMutatorBuilder ReduceGroupToSingleRow(this IFluentProcessMutatorBuilder builder, ReduceGroupToSingleRowMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}