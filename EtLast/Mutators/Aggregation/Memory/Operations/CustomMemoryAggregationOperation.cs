namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class CustomMemoryAggregationOperation : AbstractMemoryAggregationOperation
    {
        public delegate void CustomMemoryAggregationOperationDelegate(List<IReadOnlySlimRow> rows, Func<SlimRow> aggregateCreator);
        public CustomMemoryAggregationOperationDelegate Delegate { get; set; }

        public override void TransformGroup(List<IReadOnlySlimRow> rows, Func<SlimRow> aggregateCreator)
        {
            Delegate?.Invoke(rows, aggregateCreator);
        }
    }
}