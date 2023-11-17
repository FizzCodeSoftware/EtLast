namespace FizzCode.EtLast;

public sealed class ThrowExceptionOnDuplicateKeyMutator(IEtlContext context) : AbstractMutator(context)
{
    [ProcessParameterMustHaveValue]
    public required Func<IReadOnlyRow, string> RowKeyGenerator { get; init; }

    private readonly HashSet<string> _keys = [];

    protected override void CloseMutator()
    {
        base.CloseMutator();

        _keys.Clear();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var key = RowKeyGenerator.Invoke(row);
        if (_keys.Contains(key))
        {
            var exception = new DuplicateKeyException(this);
            exception.Data["Key"] = key;
            exception.Data["Row"] = row.ToDebugString(true);
            exception.Data["RowInputIndex"] = rowInputIndex;
            throw exception;
        }

        _keys.Add(key);
        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ThrowExceptionOnDuplicateKeyMutatorFluent
{
    /// <summary>
    /// Throw an exception if a subsequent occurence of a row key is found.
    /// <para>- input can be unordered</para>
    /// <para>- all keys are stored in memory</para>
    /// </summary>
    public static IFluentSequenceMutatorBuilder ThrowExceptionOnDuplicateKey(this IFluentSequenceMutatorBuilder builder, ThrowExceptionOnDuplicateKeyMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
