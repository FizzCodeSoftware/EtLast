namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowValueChangedEvent : AbstractRowEvent
{
    public long? ProcessId { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
