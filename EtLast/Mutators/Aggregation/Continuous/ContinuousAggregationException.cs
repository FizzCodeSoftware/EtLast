namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ContinuousAggregationException : EtlException
{
    public ContinuousAggregationException(ContinuousAggregationMutator mutator, IContinuousAggregationOperation operation, IRow row, Exception innerException)
        : base(mutator, "error during a continuous aggregation", innerException)
    {
        Data["Operation"] = operation.GetType().GetFriendlyTypeName();
        Data["Row"] = row.ToDebugString(true);
    }
}