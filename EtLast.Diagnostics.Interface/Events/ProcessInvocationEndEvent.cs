namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessInvocationEndEvent : AbstractEvent
{
    public long InvocationId { get; set; }
    public long ElapsedMilliseconds { get; set; }
    public long? NetTimeMilliseconds { get; set; }
}
