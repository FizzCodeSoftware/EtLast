namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class MemoryAggregationException : EtlException
    {
        public MemoryAggregationException(MemoryAggregationMutator mutator, IMemoryAggregationOperation operation, List<IReadOnlySlimRow> group, Exception innerException)
            : base(mutator, "error raised during the execution of an in-memory aggregation", innerException)
        {
            Data.Add("Operation", operation.GetType().GetFriendlyTypeName());
            Data.Add("Group", string.Join("\n", group.Select(x => x.ToDebugString())));
        }
    }
}