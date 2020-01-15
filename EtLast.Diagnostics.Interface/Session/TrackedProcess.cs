namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Name}")]
    public class TrackedProcess
    {
        public string Uid { get; }
        public string Name { get; }
        public Dictionary<int, TrackedRow> AliveRowList { get; set; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; set; } = new Dictionary<int, TrackedRow>();

        public TrackedProcess(string uid, string name)
        {
            Uid = uid;
            Name = name;
        }

        public void AddRow(TrackedRow row)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentOwner = this;
        }

        public void DropRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            DroppedRowList.Add(row.Uid, row);
            row.CurrentOwner = null;
        }

        public void RemoveRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            row.CurrentOwner = null;
        }
    }
}