namespace FizzCode.EtLast;

public sealed class BatchedCompareWithRowMutator : AbstractBatchedCrossMutator
{
    public IRowEqualityComparer EqualityComparer { get; init; }
    public MatchAction MatchAndEqualsAction { get; init; }
    public MatchAction MatchButDifferentAction { get; init; }
    public NoMatchAction NoMatchAction { get; init; }

    /// <summary>
    /// The amount of rows processed in a batch. Default value is 1000.
    /// </summary>
    public override int BatchSize { get; init; } = 1000;

    public BatchedCompareWithRowMutator(IEtlContext context)
        : base(context)
    {
        UseBatchKeys = false;
    }

    protected override void MutateSingleRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
    {
        removeOriginal = false;
        processed = false;
    }

    protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
    {
        var lookup = LookupBuilder.Build(this, rows.ToArray());
        foreach (var row in rows)
        {
            var key = GenerateRowKey(row);
            var removeRow = false;
            var matchCount = lookup.CountByKey(key);
            if (matchCount == 0)
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
                // todo: handle > 1 matches
                var match = lookup.GetSingleRowByKey(key);
                var isSame = EqualityComparer.Equals(row, match);
                if (!isSame)
                {
                    if (MatchButDifferentAction != null)
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
                else if (MatchAndEqualsAction != null)
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

            if (removeRow)
                removedRows.Add(row);
            else
                mutatedRows.Add(row);
        }

        lookup.Clear();
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

        if (MatchAndEqualsAction == null && NoMatchAction == null)
            throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));

        if (MatchAndEqualsAction?.Mode == MatchMode.Custom && MatchAndEqualsAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(MatchAndEqualsAction) + "." + nameof(MatchAndEqualsAction.CustomAction));

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

        if (NoMatchAction != null && MatchAndEqualsAction != null
            && ((NoMatchAction.Mode == MatchMode.Throw && MatchAndEqualsAction.Mode == MatchMode.Throw)
                || (NoMatchAction.Mode == MatchMode.Remove && MatchAndEqualsAction.Mode == MatchMode.Remove)))
        {
            throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));
        }

        if (EqualityComparer == null)
            throw new ProcessParameterNullException(this, nameof(EqualityComparer));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class BatchedCompareWithRowMutatorFluent
{
    /// <summary>
    /// Compare input rows against existing rows with matching keys in batches and execute <see cref="CompareWithRowMutator.MatchAndEqualsAction"/> or <see cref="CompareWithRowMutator.MatchButDifferentAction"/> or <see cref="CompareWithRowMutator.NoMatchAction"/> based on the result of the comparison.
    /// - existing rows are looked up from dynamically compiled <see cref="RowLookup"/> based on a batch of input rows
    /// </summary>
    public static IFluentSequenceMutatorBuilder CompareWithRowBatched(this IFluentSequenceMutatorBuilder builder, BatchedCompareWithRowMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
