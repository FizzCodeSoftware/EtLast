namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowValueChangedEvent : AbstractRowEvent
{
    public long? ProcessInvocationUID { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
