namespace FizzCode.EtLast
{
    public interface IContinuousAggregationOperation
    {
        IProcess Process { get; }
        void SetProcess(IProcess process);
        void TransformGroup(string[] groupingColumns, IRow row, IRow aggregateRow, int rowsInGroup);
    }
}