namespace FizzCode.EtLast;

public interface IMemoryAggregationOperation
{
    AbstractMemoryAggregationMutator Process { get; }
    void SetProcess(AbstractMemoryAggregationMutator process);
    void TransformGroup(List<IReadOnlySlimRow> groupRows, Func<ISlimRow> aggregateCreator);
}
