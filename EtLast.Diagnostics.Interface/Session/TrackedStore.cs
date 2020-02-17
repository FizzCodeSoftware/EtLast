namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Path}")]
    public class TrackedStore
    {
        public int UID { get; }
        public KeyValuePair<string, string>[] Descriptor { get; }
        public int RowCount { get; set; }

        public TrackedStore(int uid, KeyValuePair<string, string>[] descriptor)
        {
            UID = uid;
            Descriptor = descriptor;
        }
    }
}