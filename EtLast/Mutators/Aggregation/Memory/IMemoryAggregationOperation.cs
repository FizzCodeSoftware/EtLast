namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMemoryAggregationOperation
    {
        MemoryAggregationMutator Process { get; }
        void SetProcess(MemoryAggregationMutator process);
        void TransformGroup(List<IReadOnlySlimRow> rows, SlimRow aggregate);
    }
}