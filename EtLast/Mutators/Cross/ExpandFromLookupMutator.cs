namespace FizzCode.EtLast;

public sealed class ExpandFromLookupMutator : AbstractCrossMutator
{
    [ProcessParameterMustHaveValue]
    public required Dictionary<string, string> Columns { get; init; }

    [ProcessParameterMustHaveValue]
    public required SelectRowFromLookupDelegate MatchSelector { get; init; }

    public NoMatchAction NoMatchAction { get; init; }
    public MatchActionDelegate MatchCustomAction { get; init; }

    private RowLookup _lookup;
    private List<KeyValuePair<string, object>> _changes;

    public ExpandFromLookupMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _lookup = LookupBuilder.Build(this);
        _changes = new List<KeyValuePair<string, object>>();
    }

    protected override void CloseMutator()
    {
        _lookup.Clear();
        _lookup = null;

        _changes.Clear();
        _changes = null;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var removeRow = false;
        var match = MatchSelector(row, _lookup);
        if (match == null)
        {
            if (NoMatchAction != null)
            {
                switch (NoMatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        throw new NoMatchException(this, row);
                    case MatchMode.Custom:
                        NoMatchAction.InvokeCustomAction(row);
                        break;
                    case MatchMode.CustomThenRemove:
                        removeRow = true;
                        NoMatchAction.InvokeCustomAction(row);
                        break;
                }
            }
        }
        else
        {
            _changes.Clear();
            foreach (var column in Columns)
            {
                _changes.Add(new KeyValuePair<string, object>(column.Key, match[column.Value ?? column.Key]));
            }
            row.MergeWith(_changes);

            MatchCustomAction?.Invoke(row, match);
        }

        if (!removeRow)
            yield return row;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ExpandFromLookupMutatorFluent
{
    /// <summary>
    /// Copy columns to input rows from existing rows using a custom selector.
    /// - <see cref="ExpandFromLookupMutator.MatchSelector"/> can select 0 or 1 row from a single <see cref="RowLookup"/> for each row
    /// </summary>
    public static IFluentSequenceMutatorBuilder ExpandFromLookup(this IFluentSequenceMutatorBuilder builder, ExpandFromLookupMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
