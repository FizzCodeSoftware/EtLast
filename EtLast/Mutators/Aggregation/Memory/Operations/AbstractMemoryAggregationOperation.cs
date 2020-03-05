namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractMemoryAggregationOperation : IMemoryAggregationOperation
    {
        public MemoryAggregationMutator Process { get; private set; }

        public abstract void TransformGroup(List<IReadOnlyRow> rows, SlimRow aggregate);

        public void SetProcess(MemoryAggregationMutator process)
        {
            Process = process;
        }
    }
}