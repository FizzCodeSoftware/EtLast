namespace FizzCode.EtLast
{
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public ContinuousAggregationMutator Process { get; private set; }

        public abstract void TransformAggregate(IReadOnlySlimRow row, ContinuousAggregate aggregate);

        public void SetProcess(ContinuousAggregationMutator process)
        {
            Process = process;
        }
    }
}