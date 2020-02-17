namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoreStartedEvent : AbstractRowEvent
    {
        public int UID { get; set; }
        public KeyValuePair<string, string>[] Descriptor { get; set; }
    }
}