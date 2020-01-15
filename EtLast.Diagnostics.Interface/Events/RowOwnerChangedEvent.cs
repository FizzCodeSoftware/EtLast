namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }

        public string PreviousProcessUid { get; set; }
        public string PreviousProcessName { get; set; }
        public string NewProcessUid { get; set; }
        public string NewProcessName { get; set; }
    }
}