namespace FizzCode.EtLast;

public sealed class ReplaceErrorWithValueMutator : AbstractSimpleChangeMutator
{
    public string[] Columns { get; init; }
    public object Value { get; init; }

    public ReplaceErrorWithValueMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        Changes.Clear();

        if (Columns != null)
        {
            foreach (var column in Columns)
            {
                if (row[column] is EtlRowError)
                {
                    Changes.Add(new KeyValuePair<string, object>(column, Value));
                }
            }
        }
        else
        {
            foreach (var kvp in row.Values)
            {
                if (kvp.Value is EtlRowError)
                {
                    Changes.Add(new KeyValuePair<string, object>(kvp.Key, Value));
                }
            }
        }

        row.MergeWith(Changes);

        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ReplaceErrorWithValueMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ReplaceErrorWithValue(this IFluentSequenceMutatorBuilder builder, ReplaceErrorWithValueMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
