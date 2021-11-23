namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IEtlTask : IProcess
    {
        IEtlSession Session { get; }
        IExecutionStatistics Statistics { get; }

        Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; }

        TaskResult Execute(IProcess caller, IEtlSession session);

        Dictionary<string, object> Output { get; }
    }
}