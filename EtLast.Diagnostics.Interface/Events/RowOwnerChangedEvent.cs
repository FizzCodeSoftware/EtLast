namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowOwnerChangedEvent : AbstractRowEvent
{
    public long PreviousProcessInvocationId { get; set; }
    public long? NewProcessInvocationId { get; set; }
}
