namespace FizzCode.EtLast;

public sealed class RemoveColumnMutator : AbstractSimpleChangeMutator
{
    [ProcessParameterMustHaveValue]
    public required string[] Columns { get; init; }

    protected override void StartMutator()
    {
        base.StartMutator();

        if (Columns.Length > 1)
        {
            Changes.AddRange(Columns.Select(x => new KeyValuePair<string, object>(x, new EtlRowRemovedValue())));
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        if (Columns.Length > 1)
        {
            row.MergeWith(Changes);
        }
        else
        {
            row[Columns[0]] = new EtlRowRemovedValue();
        }

        yield return row;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class RemoveColumnMutatorFluent
{
    public static IFluentSequenceMutatorBuilder RemoveColumn(this IFluentSequenceMutatorBuilder builder, RemoveColumnMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder RemoveColumn(this IFluentSequenceMutatorBuilder builder, params string[] columns)
    {
        return builder.AddMutator(new RemoveColumnMutator()
        {
            Columns = columns,
        });
    }
}
