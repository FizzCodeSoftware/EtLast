namespace FizzCode.EtLast;

public sealed class RemoveRowWithErrorMutator : AbstractMutator
{
    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        if (!row.HasError())
            yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RemoveRowWithErrorMutatorFluent
{
    public static IFluentSequenceMutatorBuilder RemoveRow(this IFluentSequenceMutatorBuilder builder, RemoveRowWithErrorMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
