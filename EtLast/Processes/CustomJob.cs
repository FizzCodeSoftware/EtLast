﻿namespace FizzCode.EtLast;

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
        catch (Exception ex) when (ex is not EtlException)
        {
            var exception = new CustomCodeException(this, "error in custom code: " + ex.Message, ex);
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

    public static IFlow CustomJob(this IFlow builder, string name, LogSeverity callLogSeverity, Action<CustomJob> action)
    {
        return builder.ExecuteProcess(() => new CustomJob()
        {
            Name = name,
            CallLogSeverity = callLogSeverity,
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

    public static IFlow CustomJob(this IFlow builder, string name, LogSeverity callLogSeverity, Action action)
    {
        return builder.ExecuteProcess(() => new CustomJob()
        {
            Name = name,
            CallLogSeverity = callLogSeverity,
            Action = _ => action?.Invoke(),
        });
    }
}