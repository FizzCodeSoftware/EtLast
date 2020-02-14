namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowValueChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public int? ProcessInvocationUID { get; set; }
        public KeyValuePair<string, object>[] Values { get; set; }
    }
}