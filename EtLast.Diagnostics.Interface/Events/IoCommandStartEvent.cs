namespace FizzCode.EtLast.Diagnostics.Interface;

public class IoCommandStartEvent : IoCommandEvent
{
    public long ProcessId { get; set; }
    public int? TimeoutSeconds { get; set; }
    public string Command { get; set; }
    public string TransactionId { get; set; }
    public IoCommandKind Kind { get; set; }
    public string Location { get; set; }
    public string Path { get; set; }
    public KeyValuePair<string, object>[] Arguments { get; set; }
}
