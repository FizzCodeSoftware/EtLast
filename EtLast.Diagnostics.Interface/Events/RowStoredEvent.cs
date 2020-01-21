namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoredEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public List<KeyValuePair<string, string>> Locations { get; set; }
        public int ProcessUid { get; set; }
        public OperationInfo Operation { get; set; }
    }
}