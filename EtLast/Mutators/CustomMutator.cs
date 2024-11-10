namespace FizzCode.EtLast;

public delegate bool CustomMutatorDelegate(IRow row);

public sealed class CustomMutator : AbstractMutator
{
    [ProcessParameterMustHaveValue]
    public required CustomMutatorDelegate Action { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        bool keep;
        if (Context.RowListeners.Count > 0)
        {
            var tracker = new TrackedRow(row);
            try
            {
                keep = Action.Invoke(tracker);
                if (keep)
                    tracker.ApplyChanges();
            }
            catch (Exception ex) when (ex is not EtlException)
            {
                var exception = new CustomCodeException(this, "error in custom code", ex);
                exception.Data["RowInputIndex"] = rowInputIndex;
                exception.Data["Row"] = row.ToDebugString(true);
                throw exception;
            }
        }
        else
        {
            try
            {
                keep = Action.Invoke(row);
            }
            catch (Exception ex) when (ex is not EtlException)
            {
                var exception = new CustomCodeException(this, "error in custom code", ex);
                exception.Data["RowInputIndex"] = rowInputIndex;
                exception.Data["Row"] = row.ToDebugString(true);
                throw exception;
            }
        }

        if (keep)
            yield return row;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomMutatorFluent
{
    public static IFluentSequenceMutatorBuilder CustomCode(this IFluentSequenceMutatorBuilder builder, CustomMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder CustomCode(this IFluentSequenceMutatorBuilder builder, string name, Action<IRow> action)
    {
        return builder.AddMutator(new CustomMutator()
        {
            Name = name,
            Action = row =>
            {
                action.Invoke(row);
                return true;
            }
        });
    }

    public static IFluentSequenceMutatorBuilder CustomCode(this IFluentSequenceMutatorBuilder builder, string name, CustomMutatorDelegate action)
    {
        return builder.AddMutator(new CustomMutator()
        {
            Name = name,
            Action = action,
        });
    }
}