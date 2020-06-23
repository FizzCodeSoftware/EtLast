namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Input can be unordered.
    /// - discards input rows on-the-fly
    /// - keeps already yielded row KEYS in memory (!)
    /// </summary>
    public class RemoveDuplicateRowsMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string[] KeyColumns { get; set; }

        /// <summary>
        /// Please note that only the values of the first occurence of a key will be returned.
        /// </summary>
        public string[] ValueColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        public RemoveDuplicateRowsMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        private string GetKey(IReadOnlySlimRow row)
        {
            if (KeyColumns.Length == 1)
            {
                var value = row[KeyColumns[0]];

                return value != null
                    ? DefaultValueFormatter.Format(value)
                    : "\0";
            }

            _keyBuilder.Clear();
            for (var i = 0; i < KeyColumns.Length; i++)
            {
                var value = row[KeyColumns[i]];

                if (value != null)
                    _keyBuilder.Append(DefaultValueFormatter.Format(value));

                _keyBuilder.Append('\0');
            }

            return _keyBuilder.ToString();
        }

        protected override void ValidateImpl()
        {
            if (KeyColumns == null || KeyColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(KeyColumns));
        }

        protected override IEnumerable<IRow> EvaluateImpl(Stopwatch netTimeStopwatch)
        {
            var returnedKeys = new HashSet<string>();

            netTimeStopwatch.Stop();
            var enumerator = InputProcess.Evaluate(this).TakeRowsAndTransferOwnership().GetEnumerator();
            netTimeStopwatch.Start();

            var allColumns = (ValueColumns != null
                    ? KeyColumns.Concat(ValueColumns)
                    : KeyColumns)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

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
                var key = GetKey(row);
                if (!returnedKeys.Contains(key))
                {
                    foreach (var kvp in row.Values)
                    {
                        if (!allColumns.Contains(kvp.Key))
                        {
                            row.SetStagedValue(kvp.Key, null);
                        }
                    }

                    row.ApplyStaging();

                    netTimeStopwatch.Stop();
                    yield return row;
                    netTimeStopwatch.Start();

                    returnedKeys.Add(key);
                }
                else
                {
                    Context.SetRowOwner(row, null);
                }
            }

            netTimeStopwatch.Stop();
            Context.Log(LogSeverity.Debug, this, "evaluated {RowCount} input rows and returned {ResultRowCount} rows in {Elapsed}/{ElapsedWallClock}",
                rowCount, returnedKeys.Count, InvocationInfo.LastInvocationStarted.Elapsed, netTimeStopwatch.Elapsed);

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
}