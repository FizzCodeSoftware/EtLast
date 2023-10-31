namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowOwnerChangedEvent : AbstractRowEvent
{
    public long PreviousProcessInvocationUid { get; set; }
    public long? NewProcessInvocationUid { get; set; }
}
