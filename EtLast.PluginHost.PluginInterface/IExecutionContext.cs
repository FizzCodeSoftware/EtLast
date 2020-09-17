namespace FizzCode.EtLast.PluginHost
{
    using System;

    public interface IExecutionContext
    {
        string SessionId { get; }
        IExecutionContext ParentContext { get; }
        ITopic Topic { get; }
        string Name { get; }

        string ModuleName { get; }
        string PluginName { get; }

        TimeSpan CpuTimeStart { get; }
        long TotalAllocationsStart { get; }
        long AllocationDifferenceStart { get; }

        TimeSpan CpuTimeFinish { get; }
        long TotalAllocationsFinish { get; }
        long AllocationDifferenceFinish { get; }

        TimeSpan RunTime { get; }
    }
}