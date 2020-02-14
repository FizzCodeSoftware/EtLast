namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoredEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public int ProcessInvocationUID { get; set; }
        public KeyValuePair<string, string>[] Locations { get; set; }
    }
}