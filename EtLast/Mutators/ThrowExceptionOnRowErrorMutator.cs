namespace FizzCode.EtLast;

public sealed class ThrowExceptionOnRowErrorMutator : AbstractMutator
{
    private int _rowIndex;

    public ThrowExceptionOnRowErrorMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        base.StartMutator();

        _rowIndex = 0;
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (row.HasError())
        {
            var exception = new RowContainsErrorException(this, row);
            exception.Data["RowIndex"] = _rowIndex;

            var index = 0;
            foreach (var kvp in row.Values)
            {
                if (kvp.Value is EtlRowError)
                {
                    var error = kvp.Value as EtlRowError;
                    exception.Data["Column" + index.ToString("D", CultureInfo.InvariantCulture)] = kvp.Key;
                    exception.Data["Value" + index.ToString("D", CultureInfo.InvariantCulture)] = error.OriginalValue != null
                        ? error.OriginalValue + " (" + error.OriginalValue.GetType().GetFriendlyTypeName() + ")"
                        : "NULL";
                    index++;
                }
            }

            throw exception;
        }

        _rowIndex++;
        yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class ThrowExceptionOnRowErrorMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ThrowExceptionOnRowError(this IFluentSequenceMutatorBuilder builder, ThrowExceptionOnRowErrorMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder ThrowExceptionOnRowError(this IFluentSequenceMutatorBuilder builder)
    {
        return builder.AddMutator(new ThrowExceptionOnRowErrorMutator(builder.ProcessBuilder.Result.Context));
    }
}
