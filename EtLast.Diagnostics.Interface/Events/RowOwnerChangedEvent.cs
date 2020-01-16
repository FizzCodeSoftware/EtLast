namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public ProcessInfo PreviousProcess { get; set; }
        public ProcessInfo NewProcess { get; set; }
    }
}