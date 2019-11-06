namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomExpandFromDictionaryOperation : AbstractCrossOperation
    {
        public RowTestDelegate If { get; set; }
        public MatchingRowFromDictionarySelector MatchingRowSelector { get; set; }
        public MatchKeySelector RightKeySelector { get; set; }
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public MatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        private readonly Dictionary<string, IRow> _dictionary = new Dictionary<string, IRow>();

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
            {
                Stat.IncrementDebugCounter("ignored", 1);
                return;
            }

            Stat.IncrementDebugCounter("processed", 1);

            var rightRow = MatchingRowSelector(row, _dictionary);
            if (rightRow == null)
            {
                if (NoMatchAction != null)
                {
                    switch (NoMatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            Process.RemoveRow(row, this);
                            break;
                        case MatchMode.Throw:
                            var exception = new OperationExecutionException(Process, this, row, "no match");
                            throw exception;
                        case MatchMode.Custom:
                            NoMatchAction.CustomAction.Invoke(this, row, null);
                            break;
                    }
                }

                return;
            }

            HandleMatch(row, rightRow);
        }

        private void HandleMatch(IRow row, IRow match)
        {
            foreach (var config in ColumnConfiguration)
            {
                config.Copy(this, match, row);
            }

            MatchCustomAction?.Invoke(this, row, match);
        }

        public override void Prepare()
        {
            base.Prepare();

            if (MatchingRowSelector == null)
                throw new OperationParameterNullException(this, nameof(MatchingRowSelector));

            if (RightKeySelector == null)
                throw new OperationParameterNullException(this, nameof(RightKeySelector));

            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new OperationParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            Process.Context.Log(LogSeverity.Information, Process, null, this, "evaluating <{InputProcess}>",
                RightProcess.Name);

            _dictionary.Clear();
            var rightRows = RightProcess.Evaluate(Process);
            var rightRowCount = 0;
            foreach (var row in rightRows)
            {
                rightRowCount++;
                var key = GetRightKey(Process, row);
                if (string.IsNullOrEmpty(key))
                    continue;

                _dictionary[key] = row;
            }

            Process.Context.Log(LogSeverity.Debug, Process, null, this, "fetched {RowCount} rows, dictionary size is {DictionarySize}",
                rightRowCount, _dictionary.Count);

            Stat.IncrementCounter("right rows loaded", rightRowCount);
        }

        public override void Shutdown()
        {
            base.Shutdown();
            _dictionary.Clear();
        }

        protected string GetRightKey(IProcess process, IRow row)
        {
            try
            {
                return RightKeySelector(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new OperationExecutionException(process, this, row, nameof(RightKeySelector) + " failed");
                throw exception;
            }
        }
    }
}