namespace FizzCode.EtLast;

public interface IMemoryAggregationOperation
{
    void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator);
}
