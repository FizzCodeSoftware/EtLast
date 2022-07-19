namespace FizzCode.EtLast;

public sealed class RemoveRowMutator : AbstractMutator
{
    public RemoveRowMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        return Enumerable.Empty<IRow>();
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

        if (RowFilter == null && RowTagFilter == null)
            throw new ProcessParameterNullException(this, nameof(RowFilter) + " and " + nameof(RowTagFilter));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RemoveRowMutatorFluent
{
    public static IFluentSequenceMutatorBuilder RemoveRow(this IFluentSequenceMutatorBuilder builder, RemoveRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder RemoveRow(this IFluentSequenceMutatorBuilder builder, string name, RowTestDelegate rowTestDelegate)
    {
        return builder.AddMutator(new RemoveRowMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = name,
            RowFilter = rowTestDelegate,
        });
    }

    public static IFluentSequenceMutatorBuilder RemoveAllRow(this IFluentSequenceMutatorBuilder builder)
    {
        return builder.AddMutator(new RemoveRowMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = nameof(RemoveAllRow),
        });
    }
}
