namespace FizzCode.EtLast;

public sealed class CompareWithRowMutator : AbstractCrossMutator
{
    [ProcessParameterMustHaveValue]
    public required RowKeyGenerator RowKeyGenerator { get; init; }

    [ProcessParameterMustHaveValue]
    public required IRowEqualityComparer EqualityComparer { get; init; }

    public MatchAction MatchAndEqualsAction { get; init; }
    public MatchAction MatchButDifferentAction { get; init; }
    public NoMatchAction NoMatchAction { get; init; }

    private RowLookup _lookup;

    public CompareWithRowMutator(IEtlContext context)
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
        var removeRow = false;
        var key = GenerateRowKey(row);
        if (_lookup.CountByKey(key) == 0)
        {
            if (NoMatchAction != null)
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
        }
        else
        {
            var match = _lookup.GetSingleRowByKey(key);
            var isSame = EqualityComparer.Equals(row, match);
            if (isSame)
            {
                if (MatchAndEqualsAction != null)
                {
                    switch (MatchAndEqualsAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            throw new MatchException(this, row, key);
                        case MatchMode.Custom:
                            MatchAndEqualsAction.InvokeCustomAction(row, match);
                            break;
                        case MatchMode.CustomThenRemove:
                            removeRow = true;
                            MatchAndEqualsAction.InvokeCustomAction(row, match);
                            break;
                    }
                }
            }
            else if (MatchButDifferentAction != null)
            {
                switch (MatchButDifferentAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        throw new NoMatchException(this, row, key);
                    case MatchMode.Custom:
                        MatchButDifferentAction.InvokeCustomAction(row, match);
                        break;
                    case MatchMode.CustomThenRemove:
                        removeRow = true;
                        MatchButDifferentAction.InvokeCustomAction(row, match);
                        break;
                }
            }
        }

        if (!removeRow)
            yield return row;
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (MatchAndEqualsAction == null && NoMatchAction == null && MatchButDifferentAction == null)
            throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction) + " or " + nameof(MatchButDifferentAction));

        if (MatchAndEqualsAction?.Mode == MatchMode.Custom && MatchAndEqualsAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(MatchAndEqualsAction) + "." + nameof(MatchAndEqualsAction.CustomAction));

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

        if (NoMatchAction != null && MatchAndEqualsAction != null
            && ((NoMatchAction.Mode == MatchMode.Remove && MatchAndEqualsAction.Mode == MatchMode.Remove)
                || (NoMatchAction.Mode == MatchMode.Throw && MatchAndEqualsAction.Mode == MatchMode.Throw)))
        {
            throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action mode: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));
        }
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
public static class CompareWithRowMutatorFluent
{
    /// <summary>
    /// Compare input rows against existing rows with matching keys and execute <see cref="CompareWithRowMutator.MatchAndEqualsAction"/> or <see cref="CompareWithRowMutator.MatchButDifferentAction"/> or <see cref="CompareWithRowMutator.NoMatchAction"/> based on the result of the comparison.
    /// - existing rows are looked up from a single <see cref="RowLookup"/>
    /// </summary>
    public static IFluentSequenceMutatorBuilder CompareWithRow(this IFluentSequenceMutatorBuilder builder, CompareWithRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
