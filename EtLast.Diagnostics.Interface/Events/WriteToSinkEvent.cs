namespace FizzCode.EtLast.Diagnostics.Interface;

public class WriteToSinkEvent : AbstractRowEvent
{
    public int ProcessInvocationUID { get; set; }
    public int SinkUID { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
