namespace FizzCode.EtLast;

public sealed class AddIncrementalIntegerIdMutator(IEtlContext context) : AbstractMutator(context)
{
    [ProcessParameterMustHaveValue]
    public required string Column { get; init; }

    public required int FirstId { get; init; }

    private int _nextId;

    protected override void StartMutator()
    {
        _nextId = FirstId;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        row[Column] = _nextId;
        _nextId++;
        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class AddIncrementalIdMutatorFluent
{
    public static IFluentSequenceMutatorBuilder AddIncrementalIntegerId(this IFluentSequenceMutatorBuilder builder, AddIncrementalIntegerIdMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
