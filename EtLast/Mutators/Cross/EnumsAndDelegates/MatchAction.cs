namespace FizzCode.EtLast
{
    using System;

    public delegate void MatchActionDelegate(IRow row, IReadOnlySlimRow match);

    public sealed class MatchAction
    {
        public MatchMode Mode { get; }
        public MatchActionDelegate CustomAction { get; init; }

        public MatchAction(MatchMode mode)
        {
            Mode = mode;
        }

        public void InvokeCustomAction(IRow row, IReadOnlySlimRow match)
        {
            try
            {
                var tracker = new TrackedRow(row);
                CustomAction?.Invoke(tracker, match);
                tracker.ApplyChanges();
            }
            catch (Exception ex) when (ex is not EtlException)
            {
                throw new ProcessExecutionException(row.CurrentProcess, row, "error during the execution of a " + nameof(MatchAction) + "." + nameof(CustomAction) + " delegate", ex);
            }
        }
    }
}