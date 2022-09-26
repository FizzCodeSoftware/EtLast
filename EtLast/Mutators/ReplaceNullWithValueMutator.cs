namespace FizzCode.EtLast;

public sealed class ReplaceNullWithValueMutator : AbstractSimpleChangeMutator
{
    public string[] Columns { get; init; }
    public object Value { get; init; }

    public ReplaceNullWithValueMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (Columns.Length > 1)
        {
            Changes.Clear();
            foreach (var column in Columns)
            {
                if (!row.HasValue(column))
                {
                    Changes.Add(new KeyValuePair<string, object>(column, Value));
                }
            }

            row.MergeWith(Changes);
        }
        else if (!row.HasValue(Columns[0]))
        {
            row[Columns[0]] = Value;
        }

        yield return row;
    }

    public override void ValidateParameters()
    {
        if (Value == null)
            throw new ProcessParameterNullException(this, nameof(Value));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ReplaceNullWithValueMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ReplaceNullWithValue(this IFluentSequenceMutatorBuilder builder, ReplaceNullWithValueMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
