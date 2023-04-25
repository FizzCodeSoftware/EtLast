namespace FizzCode.EtLast;

public sealed class ProcessInvocationInfo
{
    public required int InvocationUid { get; init; }
    public required int InstanceUid { get; init; }
    public required int Number { get; init; }
    public required IProcess Caller { get; init; }
    public required Stopwatch InvocationStarted { get; init; }

    public DateTimeOffset? LastInvocationFinished { get; set; }
    public long? LastInvocationNetTimeMilliseconds { get; set; }
}
