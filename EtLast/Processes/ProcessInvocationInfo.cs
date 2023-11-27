namespace FizzCode.EtLast;

public sealed class ProcessInvocationInfo
{
    public required long InvocationUid { get; init; }
    public required long InstanceUid { get; init; }
    public required long Number { get; init; }
    public required ICaller Caller { get; init; }
    public required Stopwatch InvocationStarted { get; init; }

    public DateTimeOffset? LastInvocationFinished { get; set; }
    public long? LastInvocationNetTimeMilliseconds { get; set; }
}