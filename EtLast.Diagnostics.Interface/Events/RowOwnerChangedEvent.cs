namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowOwnerChangedEvent : AbstractRowEvent
{
    public int PreviousProcessInvocationUid { get; set; }
    public int? NewProcessInvocationUid { get; set; }
}
