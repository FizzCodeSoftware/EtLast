namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoredEvent : AbstractRowEvent
    {
        public int ProcessInvocationUID { get; set; }
        public KeyValuePair<string, string>[] Locations { get; set; }
    }
}