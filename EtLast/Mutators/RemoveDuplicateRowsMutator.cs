namespace FizzCode.EtLast;

/// <summary>
/// Input can be unordered.
/// - discards input rows on-the-fly
/// - keeps already yielded row KEYS in memory (!)
/// </summary>
public sealed class RemoveDuplicateRowsMutator : AbstractMutator
{
    public required Func<IReadOnlyRow, string> KeyGenerator { get; init; }

    private readonly HashSet<string> _returnedKeys = new();

    public RemoveDuplicateRowsMutator(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        base.ValidateParameters();

        if (KeyGenerator == null)
            throw new ProcessParameterNullException(this, nameof(KeyGenerator));
    }

    protected override void CloseMutator()
    {
        base.CloseMutator();

        _returnedKeys.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var key = KeyGenerator.Invoke(row);
        if (!_returnedKeys.Contains(key))
        {
            _returnedKeys.Add(key);

            yield return row;
        }
        else
        {
            Context.SetRowOwner(row, null);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RemoveDuplicateRowsMutatorFluent
{
    /// <summary>
    /// Keeps only the first row of each key, and discard all subsequent rows with existing keys.
    /// <para>- input can be unordered</para>
    /// <para>- if a more refined logic is required to decide which row should be kept of rows with same key then <see cref="ReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRow(IFluentSequenceMutatorBuilder, ReduceGroupToSingleRowMutator)"/> or <see cref="SortedReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRowOrdered(IFluentSequenceMutatorBuilder, SortedReduceGroupToSingleRowMutator)"/></para> can be used instead.
    /// <para>- all keys are stored in memory</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder RemoveDuplicateRows(this IFluentSequenceMutatorBuilder builder, RemoveDuplicateRowsMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    /// <summary>
    /// Keeps only the first row of each key, and discard all subsequent rows with existing keys.
    /// <para>- input can be unordered</para>
    /// <para>- if a more refined logic is required to decide which row should be kept of rows with same key then <see cref="ReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRow(IFluentSequenceMutatorBuilder, ReduceGroupToSingleRowMutator)"/> or <see cref="SortedReduceGroupToSingleRowMutatorFluent.ReduceGroupToSingleRowOrdered(IFluentSequenceMutatorBuilder, SortedReduceGroupToSingleRowMutator)"/></para> can be used instead.
    /// <para>- all keys are stored in memory</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder RemoveDuplicateRows(this IFluentSequenceMutatorBuilder builder, string name, Func<IReadOnlyRow, string> keyGenerator)
    {
        return builder.AddMutator(new RemoveDuplicateRowsMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = name,
            KeyGenerator = keyGenerator,
        });
    }
}
