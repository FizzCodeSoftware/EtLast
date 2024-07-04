namespace FizzCode.EtLast.Diagnostics.Interface;

public class SinkStartedEvent : AbstractRowEvent
{
    public long Id { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }
    public string Format { get; set; }
    public long ProcessId { get; set; }
}