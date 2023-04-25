namespace FizzCode.EtLast;

public sealed class KeyTestMutator : AbstractCrossMutator
{
    public required RowKeyGenerator RowKeyGenerator { get; init; }

    public NoMatchAction NoMatchAction { get; init; }
    public MatchAction MatchAction { get; init; }

    /// <summary>
    /// Default true. If <see cref="MatchAction.CustomAction"/> is used then setting this to false results in significantly less memory usage.
    /// </summary>
    public bool MatchActionContainsMatch { get; init; } = true;

    private ICountableLookup _lookup;

    public KeyTestMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _lookup = MatchActionContainsMatch && MatchAction?.CustomAction != null
            ? new RowLookup()
            : new CountableOnlyRowLookup();

        LookupBuilder.AddTo(_lookup, this);
    }

    protected override void CloseMutator()
    {
        _lookup.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var key = GenerateRowKey(row);
        var matchCount = _lookup.CountByKey(key);
        var removeRow = false;
        if (matchCount > 0)
        {
            if (MatchAction != null)
            {
                switch (MatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        throw new MatchException(this, row, key);
                    case MatchMode.Custom:
                        {
                            IReadOnlySlimRow match = null;
                            if (_lookup is RowLookup rl)
                            {
                                match = rl.GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                    case MatchMode.CustomThenRemove:
                        {
                            removeRow = true;

                            IReadOnlySlimRow match = null;
                            if (_lookup is RowLookup rl)
                            {
                                match = rl.GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                }
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

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (RowKeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));

        if (MatchAction == null && NoMatchAction == null)
            throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));

        if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

        if (NoMatchAction != null && MatchAction != null
            && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove)
                || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
        {
            throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
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
public static class KeyTestMutatorFluent
{
    /// <summary>
    /// Tests row keys and execute <see cref="NoMatchAction"/> or <see cref="MatchAction"/> based on the result of each row.
    /// - the existing rows are read from a single <see cref="RowLookup"/>
    /// - if MatchAction.CustomJob is not null and MatchActionContainsMatch is true then all rows of the lookup are kept in the memory, otherwise a <see cref="CountableOnlyRowLookup"/> is used.
    /// </summary>
    public static IFluentSequenceMutatorBuilder KeyTest(this IFluentSequenceMutatorBuilder builder, KeyTestMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
