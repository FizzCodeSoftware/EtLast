namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowCreatedEvent : AbstractRowEvent
{
    public int ProcessInvocationUid { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
