namespace FizzCode.EtLast.Diagnostics.Interface;

public class SinkStartedEvent : AbstractRowEvent
{
    public long UID { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }
}
