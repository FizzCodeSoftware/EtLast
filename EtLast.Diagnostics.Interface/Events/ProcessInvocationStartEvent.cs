namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessInvocationStartEvent : AbstractEvent
{
    public long InvocationUID { get; set; }
    public long InstanceUID { get; set; }
    public long InvocationCounter { get; set; }
    public string Type { get; set; }
    public string Kind { get; set; }
    public string Name { get; set; }
    public string Topic { get; set; }
    public long? CallerInvocationUID { get; set; }
}
