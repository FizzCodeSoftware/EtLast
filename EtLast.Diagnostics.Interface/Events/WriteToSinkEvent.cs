namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class WriteToSinkEvent : AbstractRowEvent
    {
        public int ProcessInvocationUID { get; set; }
        public int sinkUid { get; set; }
        public KeyValuePair<string, object>[] Values { get; set; }
    }
}