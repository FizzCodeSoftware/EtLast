namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    [DebuggerDisplay("{Name}")]
    public class TrackedProcessInvocation
    {
        public int InvocationUID { get; }
        public int InstanceUID { get; }
        public int InvocationCounter { get; }
        public int? CallerInvocationUID { get; }
        public string Type { get; }
        public string Name { get; }
        public string Topic { get; }

        public string DisplayName { get; }

        public Dictionary<int, TrackedRow> StoredRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> AliveRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; } = new Dictionary<int, TrackedRow>();

        public int PassedRowCount { get; private set; }
        public Dictionary<int, int> PassedRowCountByNextProcess { get; } = new Dictionary<int, int>();

        public int CreatedRowCount { get; private set; }

        public Dictionary<int, int> InputRowCountByByPreviousProcess { get; } = new Dictionary<int, int>();
        public int InputRowCount { get; private set; }

        public TrackedProcessInvocation(int invocationUID, int instanceUID, int invocationCounter, int? callerInvocationUID, string type, string name, string topic)
        {
            InvocationUID = invocationUID;
            InstanceUID = instanceUID;
            InvocationCounter = invocationCounter;
            CallerInvocationUID = callerInvocationUID;
            Type = type;
            Name = name;
            Topic = topic;

            DisplayName = (topic != null
                ? topic + " :: " + Name
                : name)
                + " (" + instanceUID.ToString("D", CultureInfo.InvariantCulture)
                + (InvocationCounter > 1
                    ? "/" + InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                    : "") + ")";
        }

        public void InputRow(TrackedRow row, TrackedProcessInvocation previousProcess)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentOwner = this;

            InputRowCountByByPreviousProcess.TryGetValue(previousProcess.InvocationUID, out var cnt);
            cnt++;
            InputRowCountByByPreviousProcess[previousProcess.InvocationUID] = cnt;
            InputRowCount++;
        }

        public void CreateRow(TrackedRow row)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentOwner = this;

            CreatedRowCount++;
        }

        public void DropRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            DroppedRowList.Add(row.Uid, row);
            row.CurrentOwner = null;
        }

        public void PassedRow(TrackedRow row, TrackedProcessInvocation newProcess)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            row.CurrentOwner = null;

            PassedRowCountByNextProcess.TryGetValue(newProcess.InvocationUID, out var cnt);
            cnt++;
            PassedRowCountByNextProcess[newProcess.InvocationUID] = cnt;
            PassedRowCount++;
        }

        public void StoreRow(TrackedRow row)
        {
            if (!StoredRowList.ContainsKey(row.Uid))
                StoredRowList.Add(row.Uid, row);
        }
    }
}