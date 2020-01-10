namespace FizzCode.EtLast
{
    public interface IContinuousAggregationOperation : IBaseOperation
    {
        void TransformGroup(string[] groupingColumns, IProcess process, IRow row, IRow aggregateRow, int rowsInGroup);
    }
}