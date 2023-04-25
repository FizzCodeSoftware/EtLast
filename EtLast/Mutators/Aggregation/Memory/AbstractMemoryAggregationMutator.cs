namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractMemoryAggregationMutator : AbstractAggregationMutator
{
    public required IMemoryAggregationOperation Operation { get; init; }

    protected AbstractMemoryAggregationMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (KeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(KeyGenerator));

        if (Operation == null)
            throw new ProcessParameterNullException(this, nameof(Operation));
    }
}