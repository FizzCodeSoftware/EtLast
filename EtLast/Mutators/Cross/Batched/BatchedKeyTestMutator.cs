namespace FizzCode.EtLast;

public sealed class BatchedKeyTestMutator : AbstractBatchedCrossMutator
{
    public NoMatchAction NoMatchAction { get; init; }
    public MatchAction MatchAction { get; init; }

    /// <summary>
    /// Default true. If <see cref="MatchAction.CustomAction"/> is used then setting this to false results in significantly less memory usage.
    /// </summary>
    public bool MatchActionContainsMatch { get; init; } = true;

    /// <summary>
    /// The amount of rows processed in a batch. Default value is 1000.
    /// </summary>
    public override int BatchSize { get; init; } = 1000;

    /// <summary>
    /// Default value is 100.000
    /// </summary>
    public int CacheSizeLimit { get; init; } = 100000;

    private ICountableLookup _lookup;

    public BatchedKeyTestMutator(IEtlContext context)
        : base(context)
    {
        UseBatchKeys = true;
    }

    protected override void StartMutator()
    {
        _lookup = MatchActionContainsMatch && MatchAction?.CustomAction != null
            ? new RowLookup()
            : new CountableOnlyRowLookup();

        base.StartMutator();
    }

    protected override void CloseMutator()
    {
        _lookup.Clear();

        base.CloseMutator();
    }

    protected override void MutateSingleRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
    {
        removeOriginal = false;

        if (MatchAction != null)
        {
            var key = GenerateRowKey(row);
            var matchCount = _lookup.CountByKey(key);
            if (matchCount > 0)
            {
                switch (MatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeOriginal = true;
                        break;
                    case MatchMode.Throw:
                        throw new MatchException(this, row, key);
                    case MatchMode.Custom:
                        {
                            IReadOnlySlimRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                    case MatchMode.CustomThenRemove:
                        {
                            removeOriginal = true;

                            IReadOnlySlimRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                }

                if (!removeOriginal)
                {
                    mutatedRows.Add(row);
                }

                processed = true;
                return;
            }
        }

        processed = false;
    }

    protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
    {
        LookupBuilder.Append(_lookup, this, rows.ToArray());

        foreach (var row in rows)
        {
            var key = GenerateRowKey(row);

            var removeRow = false;
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
            else if (MatchAction != null)
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
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                    case MatchMode.CustomThenRemove:
                        {
                            removeRow = true;

                            IReadOnlySlimRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(row, match);
                        }
                        break;
                }
            }

            if (removeRow)
                removedRows.Add(row);
            else
                mutatedRows.Add(row);
        }

        if (_lookup.Count >= CacheSizeLimit)
        {
            _lookup.Clear();
        }
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

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
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class BatchedKeyTestMutatorFluent
{
    /// <summary>
    /// Tests row keys in batches, and execute <see cref="NoMatchAction"/> or <see cref="MatchAction"/> based on the result of each row.
    /// - the existing rows are read from a dynamically compiled <see cref="RowLookup"/>, created for each batch based on the input rows (usually using a distinct list of foreign keys)
    /// - keeps the rows of a batch in the memory
    /// - 1 lookup is built for each batch
    /// - if MatchAction.CustomJob is not null and MatchActionContainsMatch is true then all rows of the lookup are kept in the memory, otherwise a <see cref="CountableOnlyRowLookup"/> is used.
    /// </summary>
    public static IFluentSequenceMutatorBuilder KeyTestBatched(this IFluentSequenceMutatorBuilder builder, BatchedKeyTestMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
