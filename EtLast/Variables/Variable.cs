namespace FizzCode.EtLast;

[DebuggerDisplay("{ToString()}")]
public class Variable<T>(string name, T initialValue = default)
{
    public string Name { get; } = name;
    public T Value { get; set; } = initialValue;

    public override string ToString()
    {
        return (Name ?? "var") + "=" + ValueFormatter.Default.Format(Value) + " (" + typeof(T).GetFriendlyTypeName() + ")";
    }
}