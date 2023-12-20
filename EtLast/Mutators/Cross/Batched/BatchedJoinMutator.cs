namespace FizzCode.EtLast;

public sealed class BatchedJoinMutator : AbstractBatchedCrossMutator
{
    [ProcessParameterMustHaveValue]
    public required Dictionary<string, string> Columns { get; init; }

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
    /// The amount of rows processed in a batch. Default value is 1000.
    /// </summary>
    public override int BatchSize { get; init; } = 1000;

    /// <summary>
    /// Default value is true;
    /// </summary>
    public bool CopyTag { get; init; } = true;

    public BatchedJoinMutator()
    {
        UseBatchKeys = true;
    }

    protected override string GetBatchKey(IRow row)
    {
        return GenerateRowKey(row);
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
            if (FlowState.IsTerminating)
                break;

            var key = GenerateRowKey(row);
            var removeRow = false;
            var matches = lookup.GetManyByKey(key, MatchFilter);
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
                            var exception = new TooManyMatchException(this);
                            exception.Data["Row"] = row.ToDebugString(true);
                            exception.Data["Key"] = key;
                            throw exception;
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

                    InvokeCustomMatchAction(newRow, match);

                    mutatedRows.Add(newRow);
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

            if (removeRow)
                removedRows.Add(row);
            else
                mutatedRows.Add(row);
        }

        lookup.Clear();
    }

    private void InvokeCustomMatchAction(IRow newRow, IReadOnlySlimRow match)
    {
        try
        {
            MatchCustomAction?.Invoke(newRow, match);
        }
        catch (Exception ex)
        {
            throw new JoinMatchCustomActionDelegateException(this, ex, nameof(BatchedJoinMutator) + "." + nameof(MatchCustomAction), newRow, match);
        }
    }

    protected override void ValidateMutator()
    {
        base.ValidateMutator();

        if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
            throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class BatchedJoinMutatorFluent
{
    /// <summary>
    /// Copy columns to input rows from existing rows using key matching in batches. If there are more than 1 matches for a row, then it will be duplicated for each subsequent match (like a traditional SQL join operation).
    /// - the existing rows are read from a dynamically compiled <see cref="RowLookup"/>, created for each batch based on the input rows (usually using a distinct list of foreign keys)
    /// - keeps the rows of a batch in the memory
    /// - 1 lookup is built for each batch
    /// </summary>
    public static IFluentSequenceMutatorBuilder JoinBatched(this IFluentSequenceMutatorBuilder builder, BatchedJoinMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
