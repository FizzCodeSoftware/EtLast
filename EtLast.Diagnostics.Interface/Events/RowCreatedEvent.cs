namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowCreatedEvent : AbstractRowEvent
{
    public long ProcessInvocationId { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
