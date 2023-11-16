namespace FizzCode.EtLast;

public sealed class ReplaceEmptyStringWithNullMutator(IEtlContext context) : AbstractSimpleChangeMutator(context)
{
    public required string[] Columns { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        Changes.Clear();

        if (Columns != null)
        {
            foreach (var column in Columns)
            {
                if (row[column] is string { Length: 0 })
                {
                    Changes.Add(new KeyValuePair<string, object>(column, null));
                }
            }
        }
        else
        {
            foreach (var kvp in row.Values)
            {
                if (kvp.Value is string { Length: 0 })
                {
                    Changes.Add(new KeyValuePair<string, object>(kvp.Key, null));
                }
            }
        }

        row.MergeWith(Changes);

        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ReplaceEmptyStringWithNullMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ReplaceEmptyStringWithNull(this IFluentSequenceMutatorBuilder builder, ReplaceEmptyStringWithNullMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder ReplaceEmptyStringWithNull(this IFluentSequenceMutatorBuilder builder, params string[] columns)
    {
        return builder.AddMutator(new ReplaceEmptyStringWithNullMutator(builder.ProcessBuilder.Result.Context)
        {
            Columns = columns,
        });
    }
}
