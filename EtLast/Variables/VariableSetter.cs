namespace FizzCode.EtLast;

public class VariableSetter<T> : AbstractJob
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
public static class VariableSetterFluent
{
    public static IFlow SetVariable<T>(this IFlow builder, Variable<T> variable, Func<T> valueGetter)
    {
        return builder.ExecuteProcess(() => new VariableSetter<T>()
        {
            Variable = variable,
            ValueGetter = valueGetter,
        });
    }

    public static IFlow SetVariableToFixValue<T>(this IFlow builder, Variable<T> variable, T fixValue)
    {
        return builder.ExecuteProcess(() => new VariableSetter<T>()
        {
            Variable = variable,
            ValueGetter = () => fixValue,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<int> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetter<int>()
        {
            Variable = variable,
            ValueGetter = () => variable.Value + 1,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<long> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetter<long>()
        {
            Variable = variable,
            ValueGetter = () => variable.Value + 1,
        });
    }
}