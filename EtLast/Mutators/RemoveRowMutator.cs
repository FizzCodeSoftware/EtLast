namespace FizzCode.EtLast;

public sealed class RemoveRowMutator : AbstractMutator
{
    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        return [];
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class RemoveRowMutatorFluent
{
    public static IFluentSequenceMutatorBuilder RemoveRow(this IFluentSequenceMutatorBuilder builder, RemoveRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder RemoveRow(this IFluentSequenceMutatorBuilder builder, string name, RowTestDelegate rowTestDelegate)
    {
        return builder.AddMutator(new RemoveRowMutator()
        {
            Name = name,
            RowFilter = rowTestDelegate,
        });
    }

    public static IFluentSequenceMutatorBuilder RemoveAllRow(this IFluentSequenceMutatorBuilder builder)
    {
        return builder.AddMutator(new RemoveRowMutator()
        {
            Name = nameof(RemoveAllRow),
        });
    }
}
