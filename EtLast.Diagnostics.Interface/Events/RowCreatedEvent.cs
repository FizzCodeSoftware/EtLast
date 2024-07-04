namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowCreatedEvent : AbstractRowEvent
{
    public long ProcessId { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
