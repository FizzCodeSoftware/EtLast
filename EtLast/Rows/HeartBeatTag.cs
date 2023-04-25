namespace FizzCode.EtLast;

public sealed class HeartBeatTag
{
    public required int Index { get; init; }
    public required int RowCount { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required long ElapsedMilliseconds { get; init; }
}