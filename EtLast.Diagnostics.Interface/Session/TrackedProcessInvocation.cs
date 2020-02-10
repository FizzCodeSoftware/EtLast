namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;

    [DebuggerDisplay("{Name}")]
    public class TrackedProcessInvocation
    {
        public int InvocationUID { get; }
        public int InstanceUID { get; }
        public int InvocationCounter { get; }

        public TrackedProcessInvocation Invoker { get; }
        public int ParentInvokerCount { get; }
        public string IdentedName { get; }

        public string Type { get; }
        public string ShortType { get; }
        public string Name { get; }
        public string Topic { get; }
        public ProcessKind Kind { get; }

        private TimeSpan? _elapsedMillisecondsAfterFinished;

        public TimeSpan? ElapsedMillisecondsAfterFinished
        {
            get => _elapsedMillisecondsAfterFinished;
            set
            {
                _elapsedMillisecondsAfterFinished = value;
                ElapsedMillisecondsAfterFinishedAsString = value != null
                    ? Argument.TimeSpanToString(_elapsedMillisecondsAfterFinished.Value, false)
                    : "-";
            }
        }

        public string ElapsedMillisecondsAfterFinishedAsString { get; private set; } = "-";

        public string DisplayName { get; }

        public Dictionary<int, TrackedRow> StoredRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> AliveRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; } = new Dictionary<int, TrackedRow>();

        public int PassedRowCount { get; private set; }
        public Dictionary<int, int> PassedRowCountByNextProcess { get; } = new Dictionary<int, int>();

        public int CreatedRowCount { get; private set; }

        public Dictionary<int, int> InputRowCountByByPreviousProcess { get; } = new Dictionary<int, int>();
        public int InputRowCount { get; private set; }

        public TrackedProcessInvocation(int invocationUID, int instanceUID, int invocationCounter, TrackedProcessInvocation invoker, string type, ProcessKind kind, string name, string topic)
        {
            InvocationUID = invocationUID;
            InstanceUID = instanceUID;
            InvocationCounter = invocationCounter;

            Type = type;
            ShortType = GetShortTypeName(type, "Process", "Mutator", "Scope");

            Kind = kind;
            Name = name;
            Topic = topic;

            Invoker = invoker;
            ParentInvokerCount = invoker != null
                ? invoker.ParentInvokerCount + 1
                : 0;
            IdentedName = ParentInvokerCount > 0
                ? string.Concat(Enumerable.Range(1, ParentInvokerCount).Select(x => "   ")) + Name
                : Name;

            DisplayName = (topic != null
                ? topic + " :: " + Name
                : name)
                + " (" + instanceUID.ToString("D", CultureInfo.InvariantCulture)
                + (InvocationCounter > 1
                    ? "/" + InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                    : "") + ")";
        }

        private string GetShortTypeName(string type, params string[] endings)
        {
            foreach (var ending in endings)
            {
                if (type.EndsWith(ending))
                {
                    return type.Substring(0, type.Length - ending.Length);
                }
            }

            return type;
        }

        public bool IsParent(TrackedProcessInvocation process)
        {
            var invoker = Invoker;
            while (invoker != null)
            {
                if (invoker == process)
                    return true;

                invoker = invoker.Invoker;
            }

            return false;
        }

        public string KindToString()
        {
            return Kind switch
            {
                ProcessKind.jobWithResult => "job+res.",
                ProcessKind.unknown => null,
                _ => Kind.ToString(),
            };
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