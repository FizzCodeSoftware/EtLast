namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractBatchedCrossMutator : AbstractBatchedMutator
{
    [ProcessParameterMustHaveValue]
    public required FilteredRowLookupBuilder LookupBuilder { get; init; }

    [ProcessParameterMustHaveValue]
    public required RowKeyGenerator RowKeyGenerator { get; init; }

    protected AbstractBatchedCrossMutator()
    {
    }

    protected string GenerateRowKey(IReadOnlyRow row)
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
