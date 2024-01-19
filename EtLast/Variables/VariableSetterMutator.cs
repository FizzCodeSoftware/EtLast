namespace FizzCode.EtLast;

public delegate T VariableSetterMutatorDelegate<T>(IRow row);

public class VariableSetterMutator<T> : AbstractMutator
{
    [ProcessParameterMustHaveValue]
    public required Variable<T> Variable { get; init; }

    [ProcessParameterMustHaveValue]
    public required VariableSetterMutatorDelegate<T> Setter { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var newValue = Setter.Invoke(row);
        Variable.Value = newValue;
        yield return row;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class VariableSetterMutatorFluent
{
    public static IFluentSequenceMutatorBuilder SetVariable<T>(this IFluentSequenceMutatorBuilder builder, Variable<T> variable, VariableSetterMutatorDelegate<T> setter)
    {
        return builder.AddMutator(new VariableSetterMutator<T>()
        {
            Variable = variable,
            Setter = setter,
        });
    }

    public static IFluentSequenceMutatorBuilder SetVariableToFixValue<T>(this IFluentSequenceMutatorBuilder builder, Variable<T> variable, T fixValue)
    {
        return builder.AddMutator(new VariableSetterMutator<T>()
        {
            Variable = variable,
            Setter = _ => fixValue,
        });
    }

    public static IFluentSequenceMutatorBuilder IncrementVariable(this IFluentSequenceMutatorBuilder builder, Variable<int> variable)
    {
        return builder.AddMutator(new VariableSetterMutator<int>()
        {
            Variable = variable,
            Setter = _ => variable.Value + 1,
        });
    }

    public static IFluentSequenceMutatorBuilder IncrementVariable(this IFluentSequenceMutatorBuilder builder, Variable<long> variable)
    {
        return builder.AddMutator(new VariableSetterMutator<long>()
        {
            Variable = variable,
            Setter = _ => variable.Value + 1,
        });
    }
}