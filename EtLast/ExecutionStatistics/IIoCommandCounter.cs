namespace FizzCode.EtLast;

public interface IIoCommandCounter
{
    IoCommandKind Kind { get; }
    long? AffectedDataCount { get; }
    int InvocationCount { get; }
}