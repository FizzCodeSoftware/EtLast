namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Diagnostics;

    [DebuggerDisplay("{Path}")]
    public class TrackedStore
    {
        public int UID { get; }
        public string Location { get; }
        public string Path { get; }
        public int RowCount { get; set; }

        public TrackedStore(int uid, string location, string path)
        {
            UID = uid;
            Location = location;
            Path = path;
        }
    }
}