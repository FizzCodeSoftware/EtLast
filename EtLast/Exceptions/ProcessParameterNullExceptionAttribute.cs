namespace FizzCode.EtLast;

[AttributeUsage(AttributeTargets.Property)]
public class ProcessParameterNullExceptionAttribute : Attribute
{
    public bool ThrowOnEmptyString { get; init; } = true;
    public bool ThrowOnEmptyArray { get; init; } = true;
    public bool ThrowOnEmptyCollection { get; init; } = true;
}