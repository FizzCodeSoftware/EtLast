namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Name}")]
    public class TrackedProcess
    {
        public int Uid { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
        public string Topic { get; set; }

        public Dictionary<int, TrackedRow> StoredRowList { get; set; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> AliveRowList { get; set; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; set; } = new Dictionary<int, TrackedRow>();
        public int PassedRowCount { get; private set; }
        public int CreatedRowCount { get; private set; }

        public TrackedProcess(int uid, string type, string name, string topic)
        {
            Uid = uid;
            Type = type;
            Name = name;
            Topic = topic;
        }

        public void AddRow(TrackedRow row, TrackedProcess previousProcess)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentOwner = this;

            if (previousProcess == null)
            {
                CreatedRowCount++;
            }
        }

        public void DropRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            DroppedRowList.Add(row.Uid, row);
            row.CurrentOwner = null;
        }

        public void PassedRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            row.CurrentOwner = null;
            PassedRowCount++;
        }

        public void StoreRow(TrackedRow row)
        {
            if (!StoredRowList.ContainsKey(row.Uid))
                StoredRowList.Add(row.Uid, row);
        }
    }
}