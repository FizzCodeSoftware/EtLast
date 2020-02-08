namespace FizzCode.EtLast
{
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public IProcess Process { get; private set; }

        public abstract void TransformGroup(string[] groupingColumns, IRow row, IRow aggregateRow, int rowsInGroup);

        public void SetProcess(IProcess process)
        {
            Process = process;
        }
    }
}