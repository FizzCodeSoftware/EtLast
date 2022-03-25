namespace FizzCode.EtLast;

using System;

public interface IExecutionStatistics
{
    TimeSpan CpuTimeStart { get; }
    long TotalAllocationsStart { get; }
    long AllocationDifferenceStart { get; }

    TimeSpan CpuTimeFinish { get; }
    long TotalAllocationsFinish { get; }
    long AllocationDifferenceFinish { get; }

    TimeSpan RunTime { get; }

    TimeSpan CpuTime => CpuTimeFinish.Subtract(CpuTimeStart);
    long TotalAllocations => TotalAllocationsFinish - TotalAllocationsStart;
    long AllocationDifference => AllocationDifferenceFinish - AllocationDifferenceStart;
}
