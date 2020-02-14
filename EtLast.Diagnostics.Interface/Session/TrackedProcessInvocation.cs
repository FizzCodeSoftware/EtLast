namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;

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
                    ? FormattingHelpers.TimeSpanToString(_elapsedMillisecondsAfterFinished.Value, false)
                    : "-";
            }
        }

        public string ElapsedMillisecondsAfterFinishedAsString { get; private set; } = "-";

        public string DisplayName { get; }

        public int StoredRowCount { get; private set; }

        // todo: unused
        public Dictionary<int, TrackedRow> StoredRowList { get; } = new Dictionary<int, TrackedRow>();

        public Dictionary<int, TrackedRow> AliveRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, TrackedRow> DroppedRowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<int, HashSet<int>> AliveRowsByPreviousProcess { get; } = new Dictionary<int, HashSet<int>>();

        public int PassedRowCount { get; private set; }
        public int CreatedRowCount { get; private set; }

        public Dictionary<int, int> StoredRowCountByPreviousProcess { get; } = new Dictionary<int, int>();
        public Dictionary<int, int> DroppedRowCountByPreviousProcess { get; } = new Dictionary<int, int>();
        public Dictionary<int, int> PassedRowCountByPreviousProcess { get; } = new Dictionary<int, int>();

        public Dictionary<int, int> InputRowCountByPreviousProcess { get; } = new Dictionary<int, int>();
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

        public string GetFormattedInputRowCount()
        {
            if (InputRowCount == 0)
                return "";

            if (InputRowCountByPreviousProcess.Count == 0)
                return null;

            if (InputRowCountByPreviousProcess.Count == 1)
                return InputRowCount.ToString("D", CultureInfo.InvariantCulture);

            return InputRowCount.ToString("D", CultureInfo.InvariantCulture) + " = " +
                string.Join(" + ", InputRowCountByPreviousProcess.Select(x => x.Value.FormatToStringNoZero()));
        }

        public string GetFormattedRowFlow(ExecutionContext executionContext)
        {
            var sb = new StringBuilder();

            foreach (var kvp in InputRowCountByPreviousProcess)
            {
                var inputProcess = executionContext.WholePlaybook.ProcessList[kvp.Key];

                if (sb.Length > 0)
                {
                    sb.AppendLine();
                }

                sb.AppendLine(inputProcess.DisplayName);

                sb.Append("IN: ").AppendLine(kvp.Value.FormatToString());

                if (DroppedRowCountByPreviousProcess.TryGetValue(kvp.Key, out var dropped))
                {
                    sb.Append("DROP: ").AppendLine(dropped.FormatToString());
                }

                if (StoredRowCountByPreviousProcess.TryGetValue(kvp.Key, out var stored))
                {
                    sb.Append("STORE: ").AppendLine(stored.FormatToString());
                }

                if (PassedRowCountByPreviousProcess.TryGetValue(kvp.Key, out var passed))
                {
                    sb.Append("OUT: ").AppendLine(passed.FormatToString());
                }
            }

            if (CreatedRowCount > 0)
            {
                if (sb.Length > 0)
                {
                    sb.AppendLine();
                }

                sb.AppendLine("SELF");

                sb.Append("CREATE: ").AppendLine(CreatedRowCount.FormatToString());

                var createdAndDroppedCount = DroppedRowList.Count - DroppedRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndDroppedCount > 0)
                {
                    sb.Append("DROP: ").AppendLine(createdAndDroppedCount.FormatToString());
                }

                var createdAndStoredCount = StoredRowCount - StoredRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndStoredCount > 0)
                {
                    sb.Append("STORE: ").AppendLine(createdAndStoredCount.FormatToString());
                }

                var createdAndPassedCount = PassedRowCount - PassedRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndPassedCount > 0)
                {
                    sb.Append("OUT: ").AppendLine(createdAndPassedCount.FormatToString());
                }
            }

            if (sb.Length > 0)
            {
                sb.AppendLine();
            }

            sb.AppendLine("TOTAL");
            sb.Append("IN: ").AppendLine(InputRowCount.FormatToStringNoZero());
            sb.Append("CREATE: ").AppendLine(CreatedRowCount.FormatToStringNoZero());
            sb.Append("DROP: ").AppendLine(DroppedRowList.Count.FormatToStringNoZero());
            sb.Append("STORE: ").AppendLine(StoredRowCount.FormatToStringNoZero());
            sb.Append("OUT: ").AppendLine(PassedRowCount.FormatToStringNoZero());

            return sb.ToString();
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

        public bool HasParent(TrackedProcessInvocation process)
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

        public bool HasParentWithTopic(string topic)
        {
            var invoker = Invoker;
            while (invoker != null)
            {
                if (invoker.Topic == topic)
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

            if (!AliveRowsByPreviousProcess.TryGetValue(previousProcess.InvocationUID, out var list))
            {
                list = new HashSet<int>();
                AliveRowsByPreviousProcess.Add(previousProcess.InvocationUID, list);
            }

            list.Add(row.Uid);

            row.CurrentProcess = this;

            InputRowCountByPreviousProcess.TryGetValue(previousProcess.InvocationUID, out var cnt);
            cnt++;
            InputRowCountByPreviousProcess[previousProcess.InvocationUID] = cnt;
            InputRowCount++;
        }

        public void CreateRow(TrackedRow row)
        {
            if (AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            AliveRowList.Add(row.Uid, row);
            row.CurrentProcess = this;

            CreatedRowCount++;
        }

        public void DropRow(TrackedRow row)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(row.Uid))
                {
                    DroppedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    DroppedRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(row.Uid);
                }
            }

            AliveRowList.Remove(row.Uid);
            DroppedRowList.Add(row.Uid, row);
            row.CurrentProcess = null;
        }

        public void PassedRow(TrackedRow row, TrackedProcessInvocation newProcess)
        {
            if (!AliveRowList.ContainsKey(row.Uid))
                throw new Exception("ohh");

            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(row.Uid))
                {
                    PassedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    PassedRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(row.Uid);
                }
            }

            AliveRowList.Remove(row.Uid);
            row.CurrentProcess = null;

            PassedRowCount++;
        }

        public void StoreRow(TrackedRow row, TrackedStore store)
        {
            StoredRowCount++;

            if (!StoredRowList.ContainsKey(row.Uid))
                StoredRowList.Add(row.Uid, row);

            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(row.Uid))
                {
                    StoredRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    StoredRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(row.Uid);
                }
            }
        }
    }
}