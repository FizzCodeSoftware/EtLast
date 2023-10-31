namespace FizzCode.EtLast;

public sealed class SetTagMutator : AbstractMutator
{
    public required object Tag { get; init; }

    public SetTagMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        row.Tag = Tag;
        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class SetTagMutatorFluent
{
    public static IFluentSequenceMutatorBuilder SetTag(this IFluentSequenceMutatorBuilder builder, SetTagMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
