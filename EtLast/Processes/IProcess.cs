namespace FizzCode.EtLast
{
    using System.Diagnostics;

    public interface IProcess
    {
        int InvocationUID { get; set; }
        int InstanceUID { get; set; }
        int InvocationCounter { get; set; }
        IProcess Caller { get; set; }
        Stopwatch LastInvocation { get; set; }

        IEtlContext Context { get; }
        string Name { get; }
        string Topic { get; }

        StatCounterCollection CounterCollection { get; }
    }
}