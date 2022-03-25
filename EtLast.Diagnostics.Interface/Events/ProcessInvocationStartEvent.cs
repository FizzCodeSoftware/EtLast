namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessInvocationStartEvent : AbstractEvent
{
    public int InvocationUID { get; set; }
    public int InstanceUID { get; set; }
    public int InvocationCounter { get; set; }
    public string Type { get; set; }
    public string Kind { get; set; }
    public string Name { get; set; }
    public string Topic { get; set; }
    public int? CallerInvocationUID { get; set; }
}
