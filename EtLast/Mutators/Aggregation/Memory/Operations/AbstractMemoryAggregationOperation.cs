namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public abstract class AbstractMemoryAggregationOperation : IMemoryAggregationOperation
    {
        public AbstractMemoryAggregationMutator Process { get; private set; }

        public abstract void TransformGroup(List<IReadOnlySlimRow> rows, Func<SlimRow> aggregateCreator);

        public void SetProcess(AbstractMemoryAggregationMutator process)
        {
            Process = process;
        }
    }
}