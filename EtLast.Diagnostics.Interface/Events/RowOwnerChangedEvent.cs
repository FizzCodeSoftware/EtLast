namespace FizzCode.EtLast.Diagnostics.Interface;

public class RowOwnerChangedEvent : AbstractRowEvent
{
    public long PreviousProcessId { get; set; }
    public long? NewProcessId { get; set; }
}
