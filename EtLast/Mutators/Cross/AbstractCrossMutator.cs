namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractCrossMutator : AbstractMutator
{
    public RowLookupBuilder LookupBuilder { get; init; }

    protected AbstractCrossMutator(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        if (LookupBuilder == null)
            throw new ProcessParameterNullException(this, nameof(LookupBuilder));
    }
}