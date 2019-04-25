namespace FizzCode.EtLast
{
    public class ContinuousRemoveDuplicatesOperation : AbstractContinuousAggregationOperation
    {
        public override void TransformGroup(string[] groupingColumns, IProcess process, IRow row, IRow groupRow, int rowsInGroup)
        {
        }
    }
}