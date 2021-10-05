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
        public int InvocationUid { get; }
        public int InstanceUID { get; }
        public int InvocationCounter { get; }

        public TrackedProcessInvocation Invoker { get; }
        public List<TrackedProcessInvocation> Children { get; } = new List<TrackedProcessInvocation>();
        public int ParentInvokerCount { get; }
        public string IdentedName { get; }

        public string Type { get; }
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

        private TimeSpan? _netTimeAfterFinished;

        public TimeSpan? NetTimeAfterFinished
        {
            get => _netTimeAfterFinished;
            set
            {
                _netTimeAfterFinished = value;
                NetTimeAfterFinishedAsString = value != null
                    ? FormattingHelpers.TimeSpanToString(_netTimeAfterFinished.Value, true)
                    : "-";
            }
        }

        public string NetTimeAfterFinishedAsString { get; private set; } = "-";

        public string DisplayName { get; }

        public int StoredRowCount { get; private set; }
        public int DroppedRowCount { get; private set; }

        public int AliveRowCount { get; private set; }
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
            InvocationUid = invocationUID;
            InstanceUID = instanceUID;
            InvocationCounter = invocationCounter;

            Type = type;

            Kind = kind;
            Name = name;
            Topic = topic;

            Invoker = invoker;
            Invoker?.Children.Add(this);
            Invoker?.InputRowCountByPreviousProcess.Add(InvocationUid, 0);

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
                return InputRowCount.FormatToString();

            return InputRowCount.FormatToString() + " = "
                + string.Join(" + ", InputRowCountByPreviousProcess.Where(x => x.Value > 0).Select(x => x.Value.FormatToString()));
        }

        public string GetFormattedRowFlow(DiagContext diagContext)
        {
            var sb = new StringBuilder();

            foreach (var kvp in InputRowCountByPreviousProcess)
            {
                var inputProcess = diagContext.WholePlaybook.ProcessList[kvp.Key];
                if (kvp.Value == 0
                    && !DroppedRowCountByPreviousProcess.ContainsKey(kvp.Key)
                    && !StoredRowCountByPreviousProcess.ContainsKey(kvp.Key)
                    && !PassedRowCountByPreviousProcess.ContainsKey(kvp.Key))
                {
                    continue;
                }

                if (sb.Length > 0)
                {
                    sb.AppendLine();
                }

                sb.AppendLine(inputProcess.DisplayName);

                sb.Append("IN: ").AppendLine(kvp.Value.FormatToString());

                if (DroppedRowCountByPreviousProcess.TryGetValue(kvp.Key, out var dropped))
                    sb.Append("DROP: ").AppendLine(dropped.FormatToString());

                if (StoredRowCountByPreviousProcess.TryGetValue(kvp.Key, out var stored))
                    sb.Append("STORE: ").AppendLine(stored.FormatToString());

                if (PassedRowCountByPreviousProcess.TryGetValue(kvp.Key, out var passed))
                    sb.Append("OUT: ").AppendLine(passed.FormatToString());
            }

            if (CreatedRowCount > 0)
            {
                if (sb.Length > 0)
                    sb.AppendLine();

                sb.AppendLine("SELF");

                sb.Append("CREATED: ").AppendLine(CreatedRowCount.FormatToString());

                var createdAndDroppedCount = DroppedRowCount - DroppedRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndDroppedCount > 0)
                    sb.Append("DROP: ").AppendLine(createdAndDroppedCount.FormatToString());

                var createdAndStoredCount = StoredRowCount - StoredRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndStoredCount > 0)
                    sb.Append("STORE: ").AppendLine(createdAndStoredCount.FormatToString());

                var createdAndPassedCount = PassedRowCount - PassedRowCountByPreviousProcess.Sum(x => x.Value);
                if (createdAndPassedCount > 0)
                    sb.Append("OUT: ").AppendLine(createdAndPassedCount.FormatToString());
            }

            if (InputRowCount > 0
                || CreatedRowCount > 0
                || DroppedRowCount > 0
                || StoredRowCount > 0
                || PassedRowCount > 0)
            {
                if (sb.Length > 0)
                    sb.AppendLine();

                sb.AppendLine("TOTAL");

                if (InputRowCount > 0)
                    sb.Append("IN: ").AppendLine(InputRowCount.FormatToStringNoZero());

                if (CreatedRowCount > 0)
                    sb.Append("CREATED: ").AppendLine(CreatedRowCount.FormatToStringNoZero());

                if (DroppedRowCount > 0)
                    sb.Append("DROP: ").AppendLine(DroppedRowCount.FormatToStringNoZero());

                if (StoredRowCount > 0)
                    sb.Append("STORE: ").AppendLine(StoredRowCount.FormatToStringNoZero());

                if (PassedRowCount > 0)
                    sb.Append("OUT: ").AppendLine(PassedRowCount.FormatToStringNoZero());
            }

            return sb.ToString();
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

        public void InputRow(int uid, TrackedProcessInvocation previousProcess)
        {
            AliveRowCount++;

            if (!AliveRowsByPreviousProcess.TryGetValue(previousProcess.InvocationUid, out var list))
            {
                list = new HashSet<int>();
                AliveRowsByPreviousProcess.Add(previousProcess.InvocationUid, list);
            }

            list.Add(uid);

            InputRowCountByPreviousProcess.TryGetValue(previousProcess.InvocationUid, out var cnt);
            cnt++;
            InputRowCountByPreviousProcess[previousProcess.InvocationUid] = cnt;
            InputRowCount++;
        }

        public void CreateRow()
        {
            AliveRowCount++;
            CreatedRowCount++;
        }

        public void DropRow(int uid)
        {
            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(uid))
                {
                    DroppedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    DroppedRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(uid);
                }
            }

            AliveRowCount--;
            DroppedRowCount++;
        }

        public void PassedRow(int uid)
        {
            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(uid))
                {
                    PassedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    PassedRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(uid);
                }
            }

            AliveRowCount--;
            PassedRowCount++;
        }

        public void StoreRow(int uid)
        {
            foreach (var list in AliveRowsByPreviousProcess)
            {
                if (list.Value.Contains(uid))
                {
                    StoredRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                    StoredRowCountByPreviousProcess[list.Key] = count + 1;

                    list.Value.Remove(uid);
                }
            }

            StoredRowCount++;
        }
    }
}