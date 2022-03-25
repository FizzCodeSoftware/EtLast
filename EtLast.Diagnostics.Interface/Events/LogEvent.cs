namespace FizzCode.EtLast.Diagnostics.Interface;

using System.Collections.Generic;
using FizzCode.EtLast;

public class LogEvent : AbstractEvent
{
    public int? ProcessInvocationUID { get; set; }
    public string TransactionId { get; set; }
    public string Text { get; set; }
    public LogSeverity Severity { get; set; }
    public KeyValuePair<string, object>[] Arguments { get; set; }
}
