namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandFromLookupMutator : AbstractCrossMutator
    {
        public RowTestDelegate If { get; set; }
        public MatchingRowFromLookupSelector MatchingRowSelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }

        public CustomExpandFromLookupMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", RightProcess.Name);

            var lookup = new Dictionary<string, List<IRow>>();
            var allRightRows = RightProcess.Evaluate(this).TakeRowsAndReleaseOwnership(this);
            var rightRowCount = 0;
            foreach (var row in allRightRows)
            {
                rightRowCount++;
                var key = GetRightKey(row);
                if (string.IsNullOrEmpty(key))
                    continue;

                if (!lookup.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    lookup.Add(key, list);
                }

                list.Add(row);
            }

            Context.Log(LogSeverity.Debug, this, "fetched {RowCount} rows, lookup size is {LookupSize}",
                rightRowCount, lookup.Count);

            CounterCollection.IncrementCounter("right rows loaded", rightRowCount, true);

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    CounterCollection.IncrementCounter("ignored", 1);
                    yield return row;
                    continue;
                }

                CounterCollection.IncrementCounter("processed", 1);

                var removeRow = false;
                var rightRow = MatchingRowSelector(row, lookup);
                if (rightRow == null)
                {
                    if (NoMatchAction != null)
                    {
                        switch (NoMatchAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "no match");
                                throw exception;
                            case MatchMode.Custom:
                                NoMatchAction.CustomAction.Invoke(this, row);
                                break;
                        }
                    }
                }
                else
                {
                    foreach (var config in ColumnConfiguration)
                    {
                        config.Copy(this, rightRow, row);
                    }

                    MatchCustomAction?.Invoke(this, row, rightRow);
                }

                if (removeRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }
            }

            lookup.Clear();
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (MatchingRowSelector == null)
                throw new ProcessParameterNullException(this, nameof(MatchingRowSelector));

            if (RightKeySelector == null)
                throw new ProcessParameterNullException(this, nameof(RightKeySelector));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }

        protected string GetRightKey(IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new ProcessExecutionException(this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}