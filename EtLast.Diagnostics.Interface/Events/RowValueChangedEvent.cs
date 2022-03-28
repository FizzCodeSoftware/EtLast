namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowValueChangedEvent : AbstractRowEvent
{
    public int? ProcessInvocationUID { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
