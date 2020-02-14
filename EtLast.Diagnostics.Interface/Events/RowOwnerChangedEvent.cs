namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public int PreviousProcessInvocationUID { get; set; }
        public int? NewProcessInvocationUID { get; set; }
    }
}