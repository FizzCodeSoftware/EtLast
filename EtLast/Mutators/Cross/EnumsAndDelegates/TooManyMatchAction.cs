namespace FizzCode.EtLast;

public delegate void TooManyMatchActionDelegate(IRow row, List<IReadOnlySlimRow> matches);

public sealed class TooManyMatchAction(MatchMode mode)
{
    public MatchMode Mode { get; } = mode;
    public TooManyMatchActionDelegate CustomAction { get; init; }

    public void InvokeCustomAction(IRow row, List<IReadOnlySlimRow> matches)
    {
        try
        {
            var tracker = new TrackedRow(row);
            CustomAction?.Invoke(tracker, matches);
            tracker.ApplyChanges();
        }
        catch (Exception ex)
        {
            var exception = new TooManyMatchActionDelegateException(row.Owner, ex);
            exception.Data["Row"] = row.ToDebugString(true);
            throw exception;
        }
    }
}
