namespace FizzCode.EtLast;

public sealed class AddIncrementalLongIdMutator : AbstractMutator
{
    public string Column { get; init; }

    /// <summary>
    /// Default value is 0.
    /// </summary>
    public long FirstId { get; init; }

    private long _nextId;

    public AddIncrementalLongIdMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        _nextId = FirstId;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        row[Column] = _nextId;
        _nextId++;
        yield return row;
    }

    public override void ValidateParameters()
    {
        if (string.IsNullOrEmpty(Column))
            throw new ProcessParameterNullException(this, nameof(Column));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class AddIncrementalLongIdMutatorFluent
{
    public static IFluentSequenceMutatorBuilder AddIncrementalLongId(this IFluentSequenceMutatorBuilder builder, AddIncrementalLongIdMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
