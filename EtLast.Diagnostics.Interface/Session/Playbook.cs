namespace FizzCode.EtLast.Diagnostics.Interface;

public delegate void OnEventAddedDelegate(Playbook playbook, List<AbstractEvent> abstractEvents);
public delegate void OnProcessInvokedDelegate(Playbook playbook, TrackedProcessInvocation process);
public delegate void OnSinkStartedDelegate(Playbook playbook, TrackedSink sink);
public delegate void OnWriteToSinkDelegate(Playbook playbook, TrackedSink sink, TrackedProcessInvocation process, long rowId, KeyValuePair<string, object>[] values);

public class Playbook(DiagContext context)
{
    public DiagContext DiagContext { get; } = context;

    public Dictionary<long, TrackedSink> SinkList { get; } = [];
    public Dictionary<long, TrackedProcessInvocation> ProcessList { get; } = [];

    public OnProcessInvokedDelegate OnProcessInvoked { get; set; }
    public OnEventAddedDelegate OnEventsAdded { get; set; }
    public OnSinkStartedDelegate OnSinkStarted { get; set; }
    public OnWriteToSinkDelegate OnWriteToSink { get; set; }

    public void AddEvents(IEnumerable<AbstractEvent> abstactEvents)
    {
        var newEvents = new List<AbstractEvent>();

        foreach (var abstactEvent in abstactEvents)
        {
            switch (abstactEvent)
            {
                case LogEvent evt:
                    {
                        if (evt.ProcessInvocationId != null && !ProcessList.ContainsKey(evt.ProcessInvocationId.Value))
                            continue;
                    }
                    break;
                case IoCommandStartEvent evt:
                    {
                        if (!ProcessList.ContainsKey(evt.ProcessInvocationId))
                            continue;
                    }
                    break;
                case ProcessInvocationStartEvent evt:
                    {
                        if (!ProcessList.ContainsKey(evt.InvocationId))
                        {
                            TrackedProcessInvocation invoker = null;
                            if (evt.CallerInvocationId != null && !ProcessList.TryGetValue(evt.CallerInvocationId.Value, out invoker))
                                continue;

                            var process = new TrackedProcessInvocation(evt.InvocationId, evt.ProcessId, evt.InvocationCounter, invoker, evt.Type, evt.Kind, evt.Name, evt.Topic);
                            ProcessList.Add(process.InvocationId, process);
                            OnProcessInvoked?.Invoke(this, process);
                        }
                    }
                    break;
                case ProcessInvocationEndEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.InvocationId, out var process))
                            continue;

                        process.ElapsedMillisecondsAfterFinished = TimeSpan.FromMilliseconds(evt.ElapsedMilliseconds);
                        if (evt.NetTimeMilliseconds != null)
                        {
                            process.NetTimeAfterFinished = TimeSpan.FromMilliseconds(evt.NetTimeMilliseconds.Value);
                        }
                    }
                    break;
                case RowCreatedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.ProcessInvocationId, out var process))
                            continue;

                        process.CreateRow();
                    }
                    break;
                case RowOwnerChangedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.PreviousProcessInvocationId, out var previousProcess))
                            continue;

                        TrackedProcessInvocation newProcess = null;
                        if (evt.NewProcessInvocationId != null && !ProcessList.TryGetValue(evt.NewProcessInvocationId.Value, out newProcess))
                            continue;

                        if (newProcess != null)
                        {
                            previousProcess.PassedRow(evt.RowId);
                            newProcess.InputRow(evt.RowId, previousProcess);
                        }
                        else
                        {
                            previousProcess.DropRow(evt.RowId);
                        }
                    }
                    break;
                case RowValueChangedEvent evt:
                    continue;
                case SinkStartedEvent evt:
                    {
                        var sink = new TrackedSink(evt.Id, evt.Location, evt.Path);
                        SinkList.Add(evt.Id, sink);
                        OnSinkStarted?.Invoke(this, sink);
                    }
                    break;
                case WriteToSinkEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.ProcessInvocationId, out var process))
                            continue;

                        if (!SinkList.TryGetValue(evt.SinkId, out var sink))
                            continue;

                        process.WriteRowToSink(evt.RowId);
                        OnWriteToSink?.Invoke(this, sink, process, evt.RowId, evt.Values);
                        sink.RowCount++;
                    }
                    break;
            }

            newEvents.Add(abstactEvent);
        }

        if (newEvents.Count == 0)
            return;

        OnEventsAdded?.Invoke(this, newEvents);
    }
}
