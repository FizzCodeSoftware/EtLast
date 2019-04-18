namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;

    [ComVisible(true)]
    [Serializable]
    public class ContinuousAggregationOperationExecutionException : EtlException
    {
        public ContinuousAggregationOperationExecutionException(IProcess process, IContinuousAggregationOperation operation, IRow row, IRow aggregateRow, Exception innerException)
            : base(process, "error raised during the execution of a continuous aggregation operation", innerException)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("AggregateRow", aggregateRow.ToDebugString());
            Data.Add("Row", row.ToDebugString());
        }

        protected ContinuousAggregationOperationExecutionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}