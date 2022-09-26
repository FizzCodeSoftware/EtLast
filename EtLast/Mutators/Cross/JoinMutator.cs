namespace FizzCode.EtLast;

public sealed class JoinMutator : AbstractCrossMutator
{
    public RowKeyGenerator RowKeyGenerator { get; init; }
    public Dictionary<string, string> Columns { get; init; }
    public NoMatchAction NoMatchAction { get; init; }
    public MatchActionDelegate MatchCustomAction { get; init; }

    /// <summary>
    /// Acts as a preliminary filter. Invoked for each match (if there is any) BEFORE the evaluation of the matches.
    /// </summary>
    public Func<IReadOnlySlimRow, bool> MatchFilter { get; init; }

    /// <summary>
    /// Default null. If any value is set and <see cref="TooManyMatchAction"/> is null then the excess rows will be removed, otherwise the action will be invoked.
    /// </summary>
    public int? MatchCountLimit { get; init; }

    /// <summary>
    /// Executed if the number of matches for a row exceeds <see cref="MatchCountLimit"/>.
    /// </summary>
    public TooManyMatchAction TooManyMatchAction { get; init; }

    /// <summary>
    /// Default value is true;
    /// </summary>
    public bool CopyTag { get; init; } = true;

    private RowLookup _lookup;

    public JoinMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _lookup = LookupBuilder.Build(this);
    }

    protected override void CloseMutator()
    {
        _lookup.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var key = GenerateRowKey(row);
        var removeRow = false;
        var matches = _lookup.GetManyByKey(key, MatchFilter);
        if (MatchCountLimit != null && matches?.Count > MatchCountLimit.Value)
        {
            if (TooManyMatchAction != null)
            {
                switch (TooManyMatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        throw new TooManyMatchException(this, row, key);
                    case MatchMode.Custom:
                        TooManyMatchAction.InvokeCustomAction(row, matches);
                        break;
                    case MatchMode.CustomThenRemove:
                        removeRow = true;
                        TooManyMatchAction.InvokeCustomAction(row, matches);
                        break;
                }
            }
            else
            {
                matches.RemoveRange(MatchCountLimit.Value, matches.Count - MatchCountLimit.Value);
            }
        }

        if (!removeRow && matches?.Count > 0)
        {
            removeRow = true;
            foreach (var match in matches)
            {
                var initialValues = new Dictionary<string, object>(row.Values);
                foreach (var column in Columns)
                {
                    initialValues[column.Key] = match[column.Value ?? column.Key];
                }

                var newRow = Context.CreateRow(this, initialValues);

                if (CopyTag)
                    newRow.Tag = row.Tag;

                InvokeCustomMatchAction(row, newRow, match);

                yield return newRow;
            }
        }
        else if (NoMatchAction != null)
        {
            switch (NoMatchAction.Mode)
            {
                case MatchMode.Remove:
                    removeRow = true;
                    break;
                case MatchMode.Throw:
                    throw new NoMatchException(this, row, key);
                case MatchMode.Custom:
                    NoMatchAction.InvokeCustomAction(row);
                    break;
                case MatchMode.CustomThenRemove:
                    removeRow = true;
                    NoMatchAction.InvokeCustomAction(row);
                    break;
            }
        }

        if (!removeRow)
            yield return row;
    }

    private void InvokeCustomMatchAction(IReadOnlySlimRow row, IRow newRow, IReadOnlySlimRow match)
    {
        try
        {
            MatchCustomAction?.Invoke(newRow, match);
        }
        catch (Exception ex)
        {
            throw new JoinMatchCustomActionDelegateException(this, ex, nameof(JoinMutator) + "." + nameof(MatchCustomAction), newRow, match);
        }
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (RowKeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
    }

    private string GenerateRowKey(IReadOnlyRow row)
    {
        try
        {
            return RowKeyGenerator(row);
        }
        catch (Exception ex)
        {
            throw KeyGeneratorException.Wrap(this, row, ex);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class JoinMutatorFluent
{
    /// <summary>
    /// Copy columns to input rows from existing rows using key matching. If there are more than 1 matches for a row, then it will be duplicated for each subsequent match (like a traditional SQL join operation).
    /// - the existing rows are read from a single <see cref="RowLookup"/>
    /// </summary>
    public static IFluentSequenceMutatorBuilder Join(this IFluentSequenceMutatorBuilder builder, JoinMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
