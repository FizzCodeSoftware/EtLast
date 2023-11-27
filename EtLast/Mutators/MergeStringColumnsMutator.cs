namespace FizzCode.EtLast;

public sealed class MergeStringColumnsMutator: AbstractSimpleChangeMutator
{
    [ProcessParameterMustHaveValue]
    public required string[] ColumnsToMerge { get; init; }

    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

    public required string Separator { get; init; }

    private readonly StringBuilder _sb = new();

    protected override void StartMutator()
    {
        base.StartMutator();

        Changes.AddRange(ColumnsToMerge.Select(x => new KeyValuePair<string, object>(x, null)));
        Changes.Add(new KeyValuePair<string, object>(TargetColumn, null));
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        foreach (var column in ColumnsToMerge)
        {
            if (_sb.Length > 0)
                _sb.Append(Separator);

            var value = row.GetAs<string>(column, null);
            if (!string.IsNullOrEmpty(value))
            {
                _sb.Append(value);
            }
        }

        Changes[ColumnsToMerge.Length] = new KeyValuePair<string, object>(TargetColumn, _sb.ToString());
        _sb.Clear();

        row.MergeWith(Changes);

        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class MergeStringColumnsMutatorFluent
{
    public static IFluentSequenceMutatorBuilder MergeStringColumns(this IFluentSequenceMutatorBuilder builder, MergeStringColumnsMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
