namespace FizzCode.EtLast;

public sealed class TrimStringMutator(IEtlContext context) : AbstractSimpleChangeMutator(context)
{
    public required string[] Columns { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        Changes.Clear();

        if (Columns != null)
        {
            foreach (var column in Columns)
            {
                if (row[column] is string str && !string.IsNullOrEmpty(str))
                {
                    var trimmed = str.Trim();
                    if (trimmed != str)
                    {
                        Changes.Add(new KeyValuePair<string, object>(column, trimmed));
                    }
                }
            }
        }
        else
        {
            foreach (var kvp in row.Values)
            {
                if (kvp.Value is string str && !string.IsNullOrEmpty(str))
                {
                    var trimmed = str.Trim();
                    if (trimmed != str)
                    {
                        Changes.Add(new KeyValuePair<string, object>(kvp.Key, trimmed));
                    }
                }
            }
        }

        row.MergeWith(Changes);
        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class TrimStringMutatorFluent
{
    public static IFluentSequenceMutatorBuilder TrimString(this IFluentSequenceMutatorBuilder builder, TrimStringMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder TrimString(this IFluentSequenceMutatorBuilder builder, params string[] columns)
    {
        return builder.AddMutator(new TrimStringMutator(builder.ProcessBuilder.Result.Context)
        {
            Columns = columns,
        });
    }
}
