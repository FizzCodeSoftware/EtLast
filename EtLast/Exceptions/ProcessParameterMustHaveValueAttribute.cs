namespace FizzCode.EtLast;

[AttributeUsage(AttributeTargets.Property)]
public class ProcessParameterMustHaveValueAttribute : Attribute
{
    public bool ThrowOnEmptyString { get; init; } = true;
    public bool ThrowOnEmptyArray { get; init; } = true;
    public bool ThrowOnEmptyCollection { get; init; } = true;
    public bool ThrowOnYearOneDate { get; init; } = true;
    public bool ThrowOnZeroIntegralNumeric { get; init; } = true;
}