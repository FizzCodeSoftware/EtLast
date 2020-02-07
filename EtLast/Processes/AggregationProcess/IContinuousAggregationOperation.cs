namespace FizzCode.EtLast
{
    public interface IContinuousAggregationOperation : IOperation
    {
        void TransformGroup(string[] groupingColumns, IProcess process, IRow row, IRow aggregateRow, int rowsInGroup);
    }
}