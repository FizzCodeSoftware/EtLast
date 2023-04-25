namespace FizzCode.EtLast;

public delegate bool ColumnValidationDelegate(ColumnValidationMutator process, IReadOnlySlimRow row, string column);

public sealed class ColumnValidationMutator : AbstractMutator
{
    public required string Column { get; init; }

    /// <summary>
    /// If this delegate returns false then the corresponding value of the row will be replaced with an <see cref="EtlRowError"/>
    /// </summary>
    public required ColumnValidationDelegate Test { get; init; }

    /// <summary>
    /// Error message in the <see cref="EtlRowError"/>
    /// </summary>
    public required string ErrorMessage { get; init; }

    public ColumnValidationMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var valid = Test(this, row, Column);
        if (!valid)
        {
            row[Column] = new EtlRowError(this, row[Column], ErrorMessage);
        }

        yield return row;
    }

    public override void ValidateParameters()
    {
        if (string.IsNullOrEmpty(Column))
            throw new ProcessParameterNullException(this, nameof(Column));

        if (Test == null)
            throw new ProcessParameterNullException(this, nameof(Test));

        if (string.IsNullOrEmpty(ErrorMessage))
            throw new ProcessParameterNullException(this, nameof(ErrorMessage));
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
