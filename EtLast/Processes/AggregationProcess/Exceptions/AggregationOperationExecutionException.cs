namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class AggregationOperationExecutionException : EtlException
    {
        public AggregationOperationExecutionException(IProcess process, IAggregationOperation operation, List<IRow> group, Exception innerException)
            : base(process, "error raised during the execution of an aggregation operation", innerException)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Group", string.Join("\n", group.Select(x => x.ToDebugString())));
        }
    }
}