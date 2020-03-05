namespace FizzCode.EtLast
{
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public ContinuousAggregationMutator Process { get; private set; }

        public abstract void TransformAggregate(IReadOnlyRow row, SlimRow aggregate, int rowsInGroup);

        public void SetProcess(ContinuousAggregationMutator process)
        {
            Process = process;
        }
    }
}