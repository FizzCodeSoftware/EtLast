namespace FizzCode.EtLast.Diagnostics.Interface;

public class ProcessStartEvent : AbstractEvent
{
    public long ProcessId { get; set; }
    public string Type { get; set; }
    public string Kind { get; set; }
    public string Name { get; set; }
    public long? CallerProcessId { get; set; }
}
