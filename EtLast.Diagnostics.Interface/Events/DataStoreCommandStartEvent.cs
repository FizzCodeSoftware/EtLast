namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class DataStoreCommandStartEvent : DataStoreCommandEvent
    {
        public int ProcessInvocationUid { get; set; }
        public int? TimeoutSeconds { get; set; }
        public string Command { get; set; }
        public string TransactionId { get; set; }
        public DataStoreCommandKind Kind { get; set; }
        public string Location { get; set; }
        public KeyValuePair<string, object>[] Arguments { get; set; }
    }
}