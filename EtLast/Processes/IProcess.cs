namespace FizzCode.EtLast
{
    using System.Diagnostics;

    public interface IProcess
    {
        IEtlContext Context { get; }
        string Name { get; }
        IProcess Caller { get; }
        Stopwatch LastInvocation { get; }

        public ProcessTestDelegate If { get; set; }
        void Validate();

        StatCounterCollection CounterCollection { get; }
    }
}