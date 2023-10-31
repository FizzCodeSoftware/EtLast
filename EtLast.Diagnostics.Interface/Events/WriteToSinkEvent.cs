namespace FizzCode.EtLast.Diagnostics.Interface;

public class WriteToSinkEvent : AbstractRowEvent
{
    public long ProcessInvocationUID { get; set; }
    public long SinkUID { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
