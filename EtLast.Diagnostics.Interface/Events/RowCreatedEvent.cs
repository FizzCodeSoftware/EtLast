namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowCreatedEvent : AbstractEvent
    {
        public int ProcessInvocationUID { get; set; }
        public int RowUid { get; set; }
        public KeyValuePair<string, object>[] Values { get; set; }
    }
}