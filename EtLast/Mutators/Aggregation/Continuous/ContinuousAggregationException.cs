namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ContinuousAggregationException : EtlException
{
    public ContinuousAggregationException(ContinuousAggregationMutator mutator, IContinuousAggregationOperation operation, IRow row, Exception innerException)
        : base(mutator, "error raised during the execution of a continuous aggregation", innerException)
    {
        Data.Add("Operation", operation.GetType().GetFriendlyTypeName());
        Data.Add("Row", row.ToDebugString(true));
    }
}
