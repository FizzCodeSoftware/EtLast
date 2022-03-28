namespace FizzCode.EtLast;

public delegate void NoMatchActionDelegate(IRow row);

public sealed class NoMatchAction
{
    public MatchMode Mode { get; }
    public NoMatchActionDelegate CustomAction { get; init; }

    public NoMatchAction(MatchMode mode)
    {
        Mode = mode;
    }

    public void InvokeCustomAction(IRow row)
    {
        try
        {
            var tracker = new TrackedRow(row);
            CustomAction?.Invoke(tracker);
            tracker.ApplyChanges();
        }
        catch (Exception ex)
        {
            throw new NoMatchActionDelegateException(row.CurrentProcess, row, ex);
        }
    }
}
