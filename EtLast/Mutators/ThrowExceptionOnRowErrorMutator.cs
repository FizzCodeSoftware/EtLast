namespace FizzCode.EtLast;

using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;

public sealed class ThrowExceptionOnRowErrorMutator : AbstractMutator
{
    public ThrowExceptionOnRowErrorMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        if (row.HasError())
        {
            var exception = new RowContainsErrorException(this, row);

            var index = 0;
            foreach (var kvp in row.Values)
            {
                if (kvp.Value is EtlRowError)
                {
                    var error = kvp.Value as EtlRowError;
                    exception.Data.Add("Column" + index.ToString("D", CultureInfo.InvariantCulture), kvp.Key);
                    exception.Data.Add("Value" + index.ToString("D", CultureInfo.InvariantCulture), error.OriginalValue != null
                        ? error.OriginalValue + " (" + error.OriginalValue.GetType().GetFriendlyTypeName() + ")"
                        : "NULL");
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
    public static IFluentProcessMutatorBuilder ThrowExceptionOnRowError(this IFluentProcessMutatorBuilder builder, ThrowExceptionOnRowErrorMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentProcessMutatorBuilder ThrowExceptionOnRowError(this IFluentProcessMutatorBuilder builder)
    {
        return builder.AddMutator(new ThrowExceptionOnRowErrorMutator(builder.ProcessBuilder.Result.Context));
    }
}
