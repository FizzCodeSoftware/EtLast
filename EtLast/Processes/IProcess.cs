namespace FizzCode.EtLast
{
    using System.Diagnostics;

    public interface IProcess
    {
        int UID { get; }
        IEtlContext Context { get; }
        string Name { get; }
        string Topic { get; }
        IProcess Caller { get; }
        Stopwatch LastInvocation { get; }

        public ProcessTestDelegate If { get; set; }
        void Validate();

        StatCounterCollection CounterCollection { get; }
    }
}