namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public int PreviousProcessUid { get; set; }
        public int? NewProcessUid { get; set; }
    }
}