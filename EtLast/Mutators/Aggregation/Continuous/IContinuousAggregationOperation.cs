namespace FizzCode.EtLast
{
    public interface IContinuousAggregationOperation
    {
        ContinuousAggregationMutator Process { get; }
        void SetProcess(ContinuousAggregationMutator process);
        void TransformAggregate(IRow row, ValueCollection aggregate, int rowsInGroup);
    }
}