namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast.Diagnostics.Interface;

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