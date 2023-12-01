namespace FizzCode.EtLast;

public delegate void MatchActionDelegate(IRow row, IReadOnlySlimRow match);

public sealed class MatchAction(MatchMode mode)
{
    public MatchMode Mode { get; } = mode;
    public MatchActionDelegate CustomAction { get; init; }

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
            throw new MatchActionDelegateException(row.Owner, row, ex);
        }
    }
}
