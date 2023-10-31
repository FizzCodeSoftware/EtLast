namespace FizzCode.EtLast;

public sealed class MultiplyByTagsMutator : AbstractMutator
{
    /// <summary>
    /// Default true.
    /// </summary>
    public required bool RemoveOriginalRow { get; init; } = true;

    [ProcessParameterMustHaveValue]
    public required object[] Tags { get; init; }

    public MultiplyByTagsMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (!RemoveOriginalRow)
            yield return row;

        foreach (var tag in Tags)
        {
            var newRow = Context.CreateRow(this, row);
            newRow.Tag = tag;
            yield return newRow;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class MultiplyWithTagsMutatorFluent
{
    public static IFluentSequenceMutatorBuilder CreateTaggedVersions(this IFluentSequenceMutatorBuilder builder, MultiplyByTagsMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder CreateTaggedVersions(this IFluentSequenceMutatorBuilder builder, params object[] tags)
    {
        return builder.AddMutator(new MultiplyByTagsMutator(builder.ProcessBuilder.Result.Context)
        {
            Tags = tags,
            RemoveOriginalRow = true,
        });
    }
}
