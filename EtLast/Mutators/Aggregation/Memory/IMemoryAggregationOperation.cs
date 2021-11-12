namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public interface IMemoryAggregationOperation
    {
        AbstractMemoryAggregationMutator Process { get; }
        void SetProcess(AbstractMemoryAggregationMutator process);
        void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator);
    }
}