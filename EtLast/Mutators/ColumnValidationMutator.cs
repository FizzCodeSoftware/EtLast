namespace FizzCode.EtLast;

public delegate bool ColumnValidationDelegate(ColumnValidationMutator process, IReadOnlySlimRow row, string column);

public sealed class ColumnValidationMutator(IEtlContext context) : AbstractMutator(context)
{
    [ProcessParameterMustHaveValue]
    public required string Column { get; init; }

    /// <summary>
    /// If this delegate returns false then the corresponding value of the row will be replaced with an <see cref="EtlRowError"/>
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required ColumnValidationDelegate Test { get; init; }

    /// <summary>
    /// Error message in the <see cref="EtlRowError"/>
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required string ErrorMessage { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var valid = Test(this, row, Column);
        if (!valid)
        {
            row[Column] = new EtlRowError(this, row[Column], ErrorMessage);
        }

        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ColumnValidationMutatorFluent
{
    /// <summary>
    /// Runs a test against a value in a row and if the test returns FALSE then the corresponding value will be replaced with an <see cref="EtlRowError"/>.
    /// </summary>
    public static IFluentSequenceMutatorBuilder ValidateColumn(this IFluentSequenceMutatorBuilder builder, ColumnValidationMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
