namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Path}")]
    public class TrackedStore
    {
        public string Path { get; }
        public List<Tuple<RowStoredEvent, TrackedRowSnapshot>> Rows { get; set; } = new List<Tuple<RowStoredEvent, TrackedRowSnapshot>>();

        public TrackedStore(string path)
        {
            Path = path;
        }
    }
}