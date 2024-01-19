namespace FizzCode.EtLast;

public class VariableSetterJob<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required Variable<T> Variable { get; init; }

    [ProcessParameterMustHaveValue]
    public required Func<T> ValueGetter { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var newValue = ValueGetter.Invoke();
        Variable.Value = newValue;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class VariableSetterJobFluent
{
    public static IFlow SetVariable<T>(this IFlow builder, Variable<T> variable, Func<T> valueGetter)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<T>()
        {
            Variable = variable,
            ValueGetter = valueGetter,
        });
    }

    public static IFlow SetVariableToFixValue<T>(this IFlow builder, Variable<T> variable, T fixValue)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<T>()
        {
            Variable = variable,
            ValueGetter = () => fixValue,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<int> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<int>()
        {
            Variable = variable,
            ValueGetter = () => variable.Value + 1,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<long> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<long>()
        {
            Variable = variable,
            ValueGetter = () => variable.Value + 1,
        });
    }
}