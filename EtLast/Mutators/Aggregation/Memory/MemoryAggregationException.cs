namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class MemoryAggregationException : EtlException
{
    public MemoryAggregationException(AbstractMemoryAggregationMutator mutator, IMemoryAggregationOperation operation, List<IReadOnlySlimRow> group, Exception innerException)
        : base(mutator, "error raised during the execution of an in-memory aggregation", innerException)
    {
        Data["Operation"] = operation.GetType().GetFriendlyTypeName();
        Data["Group"] = string.Join("\n", group.Select(x => x.ToDebugString()));
    }
}
