namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ContinuousAggregationException : EtlException
    {
        public ContinuousAggregationException(ContinuousAggregationMutator mutator, IContinuousAggregationOperation operation, IRow row, IRow aggregate, Exception innerException)
            : base(mutator, "error raised during the execution of a continuous aggregation", innerException)
        {
            Data.Add("Operation", operation.GetType().GetFriendlyTypeName());
            Data.Add("Aggregate", aggregate.ToDebugString());
            Data.Add("Row", row.ToDebugString());
        }
    }
}