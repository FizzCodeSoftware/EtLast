namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class CustomMemoryAggregationOperation : AbstractMemoryAggregationOperation
    {
        public delegate void CustomMemoryAggregationOperationDelegate(List<IReadOnlySlimRow> rows, SlimRow aggregate);
        public CustomMemoryAggregationOperationDelegate Delegate { get; set; }

        public override void TransformGroup(List<IReadOnlySlimRow> rows, SlimRow aggregate)
        {
            Delegate.Invoke(rows, aggregate);
        }
    }
}