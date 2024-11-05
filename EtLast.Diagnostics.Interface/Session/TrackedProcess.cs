namespace FizzCode.EtLast.Diagnostics.Interface;

[DebuggerDisplay("{Name}")]
public class TrackedProcess
{
    public long Id { get; }

    public TrackedProcess Caller { get; }
    public List<TrackedProcess> Children { get; } = [];
    public int ParentInvokerCount { get; }
    public string IdentedName { get; }

    public string Type { get; }
    public string Name { get; }
    public string Kind { get; }

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

    public long WrittenRowCount { get; private set; }
    public long DroppedRowCount { get; private set; }

    public long AliveRowCount { get; private set; }
    public Dictionary<long, HashSet<long>> AliveRowsByPreviousProcess { get; } = [];

    public int PassedRowCount { get; private set; }
    public int CreatedRowCount { get; private set; }

    public Dictionary<long, long> WrittenRowCountByPreviousProcess { get; } = [];
    public Dictionary<long, long> DroppedRowCountByPreviousProcess { get; } = [];
    public Dictionary<long, long> PassedRowCountByPreviousProcess { get; } = [];

    public Dictionary<long, long> InputRowCountByPreviousProcess { get; } = [];
    public long InputRowCount { get; private set; }

    public TrackedProcess(long processId, TrackedProcess caller, string type, string kind, string name)
    {
        Id = processId;

        Type = type;

        Kind = kind;
        Name = name;

        Caller = caller;
        Caller?.Children.Add(this);
        Caller?.InputRowCountByPreviousProcess.Add(Id, 0);

        ParentInvokerCount = caller != null
            ? caller.ParentInvokerCount + 1
            : 0;
        IdentedName = ParentInvokerCount > 0
            ? string.Concat(Enumerable.Range(1, ParentInvokerCount).Select(x => "   ")) + Name
            : Name;

        DisplayName = name
            + " (" + processId.ToString("D", CultureInfo.InvariantCulture) + ")";
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
                && !WrittenRowCountByPreviousProcess.ContainsKey(kvp.Key)
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

            if (WrittenRowCountByPreviousProcess.TryGetValue(kvp.Key, out var writtend))
                sb.Append("SINK: ").AppendLine(writtend.FormatToString());

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

            var createdAndWrittenCount = WrittenRowCount - WrittenRowCountByPreviousProcess.Sum(x => x.Value);
            if (createdAndWrittenCount > 0)
                sb.Append("SINK: ").AppendLine(createdAndWrittenCount.FormatToString());

            var createdAndPassedCount = PassedRowCount - PassedRowCountByPreviousProcess.Sum(x => x.Value);
            if (createdAndPassedCount > 0)
                sb.Append("OUT: ").AppendLine(createdAndPassedCount.FormatToString());
        }

        if (InputRowCount > 0
            || CreatedRowCount > 0
            || DroppedRowCount > 0
            || WrittenRowCount > 0
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

            if (WrittenRowCount > 0)
                sb.Append("SINK: ").AppendLine(WrittenRowCount.FormatToStringNoZero());

            if (PassedRowCount > 0)
                sb.Append("OUT: ").AppendLine(PassedRowCount.FormatToStringNoZero());
        }

        return sb.ToString();
    }

    public bool HasParent(TrackedProcess process)
    {
        var invoker = Caller;
        while (invoker != null)
        {
            if (invoker == process)
                return true;

            invoker = invoker.Caller;
        }

        return false;
    }

    public string KindToString()
    {
        return Kind switch
        {
            "jobWithResult" => "job+res.",
            "unknown" => null,
            _ => Kind,
        };
    }

    public void InputRow(long id, TrackedProcess previousProcess)
    {
        AliveRowCount++;

        if (!AliveRowsByPreviousProcess.TryGetValue(previousProcess.Id, out var list))
        {
            list = [];
            AliveRowsByPreviousProcess.Add(previousProcess.Id, list);
        }

        list.Add(id);

        InputRowCountByPreviousProcess.TryGetValue(previousProcess.Id, out var cnt);
        cnt++;
        InputRowCountByPreviousProcess[previousProcess.Id] = cnt;
        InputRowCount++;
    }

    public void CreateRow()
    {
        AliveRowCount++;
        CreatedRowCount++;
    }

    public void DropRow(long id)
    {
        foreach (var list in AliveRowsByPreviousProcess)
        {
            if (list.Value.Contains(id))
            {
                DroppedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                DroppedRowCountByPreviousProcess[list.Key] = count + 1;

                list.Value.Remove(id);
            }
        }

        AliveRowCount--;
        DroppedRowCount++;
    }

    public void PassedRow(long id)
    {
        foreach (var list in AliveRowsByPreviousProcess)
        {
            if (list.Value.Contains(id))
            {
                PassedRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                PassedRowCountByPreviousProcess[list.Key] = count + 1;

                list.Value.Remove(id);
            }
        }

        AliveRowCount--;
        PassedRowCount++;
    }

    public void WriteRowToSink(long id)
    {
        foreach (var list in AliveRowsByPreviousProcess)
        {
            if (list.Value.Contains(id))
            {
                WrittenRowCountByPreviousProcess.TryGetValue(list.Key, out var count);
                WrittenRowCountByPreviousProcess[list.Key] = count + 1;

                list.Value.Remove(id);
            }
        }

        WrittenRowCount++;
    }
}
