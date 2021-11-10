namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public delegate void TooManyMatchActionDelegate(IRow row, List<IReadOnlySlimRow> matches);

    public class TooManyMatchAction
    {
        public MatchMode Mode { get; }
        public TooManyMatchActionDelegate CustomAction { get; init; }

        public TooManyMatchAction(MatchMode mode)
        {
            Mode = mode;
        }

        public void InvokeCustomAction(IRow row, List<IReadOnlySlimRow> matches)
        {
            try
            {
                var tracker = new TrackedRow(row);
                CustomAction?.Invoke(tracker, matches);
                tracker.ApplyChanges();
            }
            catch (Exception ex) when (ex is not EtlException)
            {
                throw new ProcessExecutionException(row.CurrentProcess, row, "error during the execution of a " + nameof(TooManyMatchAction) + "." + nameof(CustomAction) + " delegate", ex);
            }
        }
    }
}