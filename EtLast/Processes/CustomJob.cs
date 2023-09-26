namespace FizzCode.EtLast;

public sealed class CustomJob : AbstractJob
{
    public required Action<CustomJob> Action { get; init; }

    public CustomJob(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            Action.Invoke(this);
        }
        catch (Exception ex)
        {
            var exception = new CustomCodeException(this, "error during the execution of custom code", ex);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomJobFluent
{
    public static IFlow CustomJob(this IFlow builder, Func<CustomJob> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }

    public static IFlow CustomJob(this IFlow builder, string name, Action<CustomJob> action)
    {
        return builder.ExecuteProcess(() => new CustomJob(builder.Context)
        {
            Name = name,
            Action = action,
        });
    }
}