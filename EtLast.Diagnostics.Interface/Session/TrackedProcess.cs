﻿namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Name}")]
    public class TrackedProcess
    {
        public int Uid { get; }
        public string Type { get; }
        public string Name { get; }
        public string Topic { get; }

        public string DisplayName { get; }

        public Dictionary<int, TrackedOperation> OperationList { get; } = new Dictionary<int, TrackedOperation>();
        public Dictionary<int, TrackedRow> StoredRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> AliveRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; } = new Dictionary<int, TrackedRow>();
        public int PassedRowCount { get; private set; }
        public Dictionary<int, int> PassedRowCountByByNextProcess { get; } = new Dictionary<int, int>();
        public int CreatedRowCount { get; private set; }
        public Dictionary<int, int> InputRowCountByByPreviousProcess { get; } = new Dictionary<int, int>();
        public int InputRowCount { get; private set; }

        public TrackedProcess(int uid, string type, string name, string topic)
        {
            Uid = uid;
            Type = type;
            Name = name;
            Topic = topic;

            DisplayName = topic != null
                ? topic + " :: " + Name
                : name;
        }

        public void AddOperation(TrackedOperation operation)
        {
            OperationList.Add(operation.Uid, operation);
        }

        public void AddRow(TrackedRow row, TrackedProcess previousProcess)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentOwner = this;

            if (previousProcess != null)
            {
                InputRowCountByByPreviousProcess.TryGetValue(previousProcess.Uid, out var cnt);
                cnt++;
                InputRowCountByByPreviousProcess[previousProcess.Uid] = cnt;
                InputRowCount++;
            }
            else
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

        public void PassedRow(TrackedRow row, TrackedProcess newProcess)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Remove(row.Uid);
            row.CurrentOwner = null;

            PassedRowCountByByNextProcess.TryGetValue(newProcess.Uid, out var cnt);
            cnt++;
            PassedRowCountByByNextProcess[newProcess.Uid] = cnt;
            PassedRowCount++;
        }

        public void StoreRow(TrackedRow row)
        {
            if (!StoredRowList.ContainsKey(row.Uid))
                StoredRowList.Add(row.Uid, row);
        }
    }
}