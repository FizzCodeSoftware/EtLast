namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent : AbstractRowEvent
    {
        public int PreviousProcessInvocationUID { get; set; }
        public int? NewProcessInvocationUID { get; set; }
    }
}