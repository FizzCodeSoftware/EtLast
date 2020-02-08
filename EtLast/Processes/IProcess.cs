namespace FizzCode.EtLast
{
    using System.Diagnostics;

    public interface IProcess
    {
        int InvocationUID { get; set; }
        int InstanceUID { get; set; }
        int InvocationCounter { get; set; }

        IEtlContext Context { get; }
        string Name { get; }
        string Topic { get; }
        IProcess Caller { get; }
        Stopwatch LastInvocation { get; }

        StatCounterCollection CounterCollection { get; }
    }
}