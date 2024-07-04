namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessEndEvent : AbstractEvent
{
    public long ProcessId { get; set; }
    public long ElapsedMilliseconds { get; set; }
    public long? NetTimeMilliseconds { get; set; }
}
