namespace FizzCode.EtLast;

public sealed class ThrowExceptionOnRowErrorMutator : AbstractMutator
{
    protected override void StartMutator()
    {
        base.StartMutator();
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        if (row.HasError())
        {
            var exception = new RowContainsErrorException(this);
            exception.Data["RowInputIndex"] = rowInputIndex;
            exception.Data["Row"] = row.ToDebugString(true);

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
        return builder.AddMutator(new ThrowExceptionOnRowErrorMutator());
    }
}
