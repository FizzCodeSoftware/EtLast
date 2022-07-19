namespace FizzCode.EtLast;

public delegate bool CustomMutatorDelegate(IRow row);

public sealed class CustomMutator : AbstractMutator
{
    public CustomMutatorDelegate Action { get; init; }

    public CustomMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var tracker = new TrackedRow(row);
        bool keep;
        try
        {
            keep = Action.Invoke(tracker);
            if (keep)
            {
                tracker.ApplyChanges();
            }
        }
        catch (Exception ex)
        {
            var exception = new CustomCodeException(this, "error during the execution of custom code", ex);
            throw exception;
        }

        if (keep)
            yield return row;
    }

    protected override void ValidateMutator()
    {
        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomMutatorFluent
{
    public static IFluentSequenceMutatorBuilder CustomCode(this IFluentSequenceMutatorBuilder builder, CustomMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder CustomCode(this IFluentSequenceMutatorBuilder builder, string name, Action<IRow> action)
    {
        return builder.AddMutator(new CustomMutator(builder.ProcessBuilder.Result.Context)
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
        return builder.AddMutator(new CustomMutator(builder.ProcessBuilder.Result.Context)
        {
            Action = action,
        });
    }
}
