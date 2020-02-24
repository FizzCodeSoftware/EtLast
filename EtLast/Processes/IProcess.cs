namespace FizzCode.EtLast
{
    public interface IProcess
    {
        ProcessInvocationInfo InvocationInfo { get; set; }

        IEtlContext Context { get; }
        ITopic Topic { get; }
        string Name { get; }

        ProcessKind Kind { get; }

        StatCounterCollection CounterCollection { get; }
    }
}