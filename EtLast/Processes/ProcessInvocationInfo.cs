namespace FizzCode.EtLast;

public sealed class ProcessInvocationInfo
{
    public required long InvocationId { get; init; }
    public required long ProcessId { get; init; }
    public required long ProcessInvocationCount { get; init; }
    public required ICaller Caller { get; init; }
    public required Stopwatch InvocationStarted { get; init; }

    public DateTimeOffset? LastInvocationFinishedUtc { get; set; }
    public DateTimeOffset? LastInvocationFinishedLocal { get; set; }
    public long? LastInvocationNetTimeMilliseconds { get; set; }
}