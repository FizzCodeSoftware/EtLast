﻿namespace FizzCode.EtLast;

public delegate void TooManyMatchActionDelegate(IRow row, List<IReadOnlySlimRow> matches);

public sealed class TooManyMatchAction
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
        catch (Exception ex)
        {
            throw new TooManyMatchActionDelegateException(row.CurrentProcess, row, ex);
        }
    }
}
