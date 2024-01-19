namespace FizzCode.EtLast;

public delegate T VariableSetterMutatorJobDelegate<T>(Variable<T> variable);

public class VariableSetterJob<T> : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required Variable<T> Variable { get; init; }

    [ProcessParameterMustHaveValue]
    public required VariableSetterMutatorJobDelegate<T> Setter { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var newValue = Setter.Invoke(Variable);
        Variable.Value = newValue;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class VariableSetterJobFluent
{
    public static IFlow SetVariable<T>(this IFlow builder, Variable<T> variable, VariableSetterMutatorJobDelegate<T> setter)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<T>()
        {
            Variable = variable,
            Setter = setter,
        });
    }

    public static IFlow SetVariableToFixValue<T>(this IFlow builder, Variable<T> variable, T fixValue)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<T>()
        {
            Variable = variable,
            Setter = variable => fixValue,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<int> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<int>()
        {
            Variable = variable,
            Setter = variable => variable.Value + 1,
        });
    }

    public static IFlow IncrementVariable(this IFlow builder, Variable<long> variable)
    {
        return builder.ExecuteProcess(() => new VariableSetterJob<long>()
        {
            Variable = variable,
            Setter = variable => variable.Value + 1,
        });
    }
}