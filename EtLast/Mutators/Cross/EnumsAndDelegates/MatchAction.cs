namespace FizzCode.EtLast;

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
        catch (Exception ex)
        {
            throw new MatchActionDelegateException(row.CurrentProcess, row, ex);
        }
    }
}
