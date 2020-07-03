namespace FizzCode.EtLast
{
    public interface IContinuousAggregationOperation
    {
        ContinuousAggregationMutator Process { get; }
        void SetProcess(ContinuousAggregationMutator process);
        void TransformAggregate(IReadOnlySlimRow row, ContinuousAggregate aggregate);
    }
}