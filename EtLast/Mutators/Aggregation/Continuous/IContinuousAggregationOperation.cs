namespace FizzCode.EtLast;

public interface IContinuousAggregationOperation
{
    void TransformAggregate(IReadOnlySlimRow row, ContinuousAggregate aggregate);
}