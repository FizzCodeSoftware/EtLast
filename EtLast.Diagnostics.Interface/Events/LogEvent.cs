namespace FizzCode.EtLast.Diagnostics.Interface;

public class LogEvent : AbstractEvent
{
    public long? ProcessInvocationId { get; set; }
    public string TransactionId { get; set; }
    public string Text { get; set; }
    public LogSeverity Severity { get; set; }
    public KeyValuePair<string, object>[] Arguments { get; set; }
}
