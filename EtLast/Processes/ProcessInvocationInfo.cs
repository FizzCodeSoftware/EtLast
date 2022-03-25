namespace FizzCode.EtLast;

using System;
using System.Diagnostics;

public sealed class ProcessInvocationInfo
{
    public int InvocationUid { get; set; }
    public int InstanceUid { get; set; }
    public int Number { get; set; }
    public IProcess Caller { get; set; }
    public Stopwatch LastInvocationStarted { get; set; }

    public DateTimeOffset? LastInvocationFinished { get; set; }
    public long? LastInvocationNetTimeMilliseconds { get; set; }
}
