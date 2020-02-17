namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoredEvent : AbstractRowEvent
    {
        public int ProcessInvocationUID { get; set; }
        public int StoreUID { get; set; }
        public KeyValuePair<string, object>[] Values { get; set; }
    }
}