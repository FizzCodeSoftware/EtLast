namespace FizzCode.EtLast.Diagnostics.Interface;

public delegate void OnEventAddedDelegate(Playbook playbook, List<AbstractEvent> abstractEvents);
public delegate void OnProcessInvokedDelegate(Playbook playbook, TrackedProcessInvocation process);
public delegate void OnSinkStartedDelegate(Playbook playbook, TrackedSink sink);
public delegate void OnWriteToSinkDelegate(Playbook playbook, TrackedSink sink, TrackedProcessInvocation process, long rowUid, KeyValuePair<string, object>[] values);

public class Playbook
{
    public DiagContext DiagContext { get; }

    public Dictionary<long, TrackedSink> SinkList { get; } = new Dictionary<long, TrackedSink>();
    public Dictionary<long, TrackedProcessInvocation> ProcessList { get; } = new Dictionary<long, TrackedProcessInvocation>();

    public OnProcessInvokedDelegate OnProcessInvoked { get; set; }
    public OnEventAddedDelegate OnEventsAdded { get; set; }
    public OnSinkStartedDelegate OnSinkStarted { get; set; }
    public OnWriteToSinkDelegate OnWriteToSink { get; set; }

    public Playbook(DiagContext context)
    {
        DiagContext = context;
    }

    public void AddEvents(IEnumerable<AbstractEvent> abstactEvents)
    {
        var newEvents = new List<AbstractEvent>();

        foreach (var abstactEvent in abstactEvents)
        {
            switch (abstactEvent)
            {
                case LogEvent evt:
                    {
                        if (evt.ProcessInvocationUID != null && !ProcessList.ContainsKey(evt.ProcessInvocationUID.Value))
                            continue;
                    }
                    break;
                case IoCommandStartEvent evt:
                    {
                        if (!ProcessList.ContainsKey(evt.ProcessInvocationUid))
                            continue;
                    }
                    break;
                case ProcessInvocationStartEvent evt:
                    {
                        if (!ProcessList.ContainsKey(evt.InvocationUID))
                        {
                            TrackedProcessInvocation invoker = null;
                            if (evt.CallerInvocationUID != null && !ProcessList.TryGetValue(evt.CallerInvocationUID.Value, out invoker))
                                continue;

                            var process = new TrackedProcessInvocation(evt.InvocationUID, evt.InstanceUID, evt.InvocationCounter, invoker, evt.Type, evt.Kind, evt.Name, evt.Topic);
                            ProcessList.Add(process.InvocationUid, process);
                            OnProcessInvoked?.Invoke(this, process);
                        }
                    }
                    break;
                case ProcessInvocationEndEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.InvocationUID, out var process))
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
                        if (!ProcessList.TryGetValue(evt.ProcessInvocationUid, out var process))
                            continue;

                        process.CreateRow();
                    }
                    break;
                case RowOwnerChangedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.PreviousProcessInvocationUid, out var previousProcess))
                            continue;

                        TrackedProcessInvocation newProcess = null;
                        if (evt.NewProcessInvocationUid != null && !ProcessList.TryGetValue(evt.NewProcessInvocationUid.Value, out newProcess))
                            continue;

                        if (newProcess != null)
                        {
                            previousProcess.PassedRow(evt.RowUid);
                            newProcess.InputRow(evt.RowUid, previousProcess);
                        }
                        else
                        {
                            previousProcess.DropRow(evt.RowUid);
                        }
                    }
                    break;
                case RowValueChangedEvent evt:
                    continue;
                case SinkStartedEvent evt:
                    {
                        var sink = new TrackedSink(evt.UID, evt.Location, evt.Path);
                        SinkList.Add(evt.UID, sink);
                        OnSinkStarted?.Invoke(this, sink);
                    }
                    break;
                case WriteToSinkEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                            continue;

                        if (!SinkList.TryGetValue(evt.SinkUID, out var sink))
                            continue;

                        process.WriteRowToSink(evt.RowUid);
                        OnWriteToSink?.Invoke(this, sink, process, evt.RowUid, evt.Values);
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
