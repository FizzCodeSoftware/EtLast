namespace FizzCode.EtLast;

public sealed class CustomJob : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required Action<CustomJob> Action { get; init; }

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

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CustomJobFluent
{
    public static IFlow CustomJob(this IFlow builder, Func<CustomJob> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }

    public static IFlow CustomJob(this IFlow builder, string name, Action<CustomJob> action)
    {
        return builder.ExecuteProcess(() => new CustomJob()
        {
            Name = name,
            Action = action,
        });
    }

    public static IFlow CustomJob(this IFlow builder, string name, Action action)
    {
        return builder.ExecuteProcess(() => new CustomJob()
        {
            Name = name,
            Action = _ => action?.Invoke(),
        });
    }
}