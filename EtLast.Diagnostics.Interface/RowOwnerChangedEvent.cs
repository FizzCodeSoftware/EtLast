namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowOwnerChangedEvent
    {
        public string[] ContextName { get; set; }
        public int RowUid { get; set; }

        public string PreviousProcessUid { get; set; }
        public string PreviousProcessName { get; set; }
        public string NewProcessUid { get; set; }
        public string NewProcessName { get; set; }
    }
}