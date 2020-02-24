namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;

    public class ProcessInvocationInfo
    {
        public int InvocationUID { get; set; }
        public int InstanceUID { get; set; }
        public int Number { get; set; }
        public IProcess Caller { get; set; }
        public Stopwatch LastInvocationStarted { get; set; }

        public DateTimeOffset? LastInvocationFinished { get; set; }
        public long? LastInvocationNetTimeMilliseconds { get; set; }
    }
}