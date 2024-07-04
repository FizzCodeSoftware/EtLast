namespace FizzCode.EtLast;

public sealed class ProcessExecutionInfo
{
    public required long Id { get; init; }
    public required ICaller Caller { get; init; }
    public required Stopwatch Timer { get; init; }

    public DateTimeOffset? FinishedOnUtc { get; set; }
    public DateTimeOffset? FinishedOnLocal { get; set; }
    public long? NetTimeMilliseconds { get; set; }
}