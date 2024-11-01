namespace FizzCode.EtLast;

public class IoCommandCounter : IIoCommandCounter
{
    public IoCommandKind Kind { get; set; }
    public int InvocationCount { get; set; }
    public long? AffectedDataCount { get; set; }
}
