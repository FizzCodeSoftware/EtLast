namespace FizzCode.EtLast;

public sealed class CustomMemoryAggregationOperation : AbstractMemoryAggregationOperation
{
    public delegate void CustomMemoryAggregationOperationDelegate(List<IReadOnlySlimRow> rows, Func<ISlimRow> aggregateCreator);
    public CustomMemoryAggregationOperationDelegate Delegate { get; set; }

    public override void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator)
    {
        Delegate?.Invoke(groupRows, aggregateCreator);
    }
}
