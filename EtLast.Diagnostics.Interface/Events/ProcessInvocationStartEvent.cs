namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessInvocationStartEvent : AbstractEvent
{
    public long InvocationId { get; set; }
    public long ProcessId { get; set; }
    public long InvocationCounter { get; set; }
    public string Type { get; set; }
    public string Kind { get; set; }
    public string Name { get; set; }
    public string Topic { get; set; }
    public long? CallerInvocationId { get; set; }
}
