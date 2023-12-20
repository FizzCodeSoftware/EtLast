namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMemoryAggregationMutator : AbstractAggregationMutator
{
    [ProcessParameterMustHaveValue]
    public required IMemoryAggregationOperation Operation { get; init; }

    protected AbstractMemoryAggregationMutator()
    {
    }

    protected override void ValidateImpl()
    {
        if (KeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(KeyGenerator));
    }
}