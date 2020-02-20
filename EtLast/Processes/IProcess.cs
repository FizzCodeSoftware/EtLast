namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public interface IProcess
    {
        int InvocationUID { get; set; }
        int InstanceUID { get; set; }
        int InvocationCounter { get; set; }
        IProcess Caller { get; set; }
        Stopwatch LastInvocationStarted { get; set; }
        DateTimeOffset? LastInvocationFinished { get; set; }

        IEtlContext Context { get; }
        ITopic Topic { get; }
        string Name { get; }

        ProcessKind Kind { get; }

        StatCounterCollection CounterCollection { get; }
    }
}