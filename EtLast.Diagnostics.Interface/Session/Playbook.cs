namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public delegate void OnEventAddedDelegate(Playbook playbook, List<AbstractEvent> abstractEvents);
    public delegate void OnProcessAddedDelegate(Playbook playbook, TrackedProcessInvocation process);
    public delegate void OnCountersUpdatedDelegate(Playbook playbook);

    public class Playbook
    {
        public ExecutionContext ExecutionContext { get; }

        public DateTime? FirstEventTimestamp { get; set; }
        public DateTime? LastEventTimestamp { get; set; }

        public Dictionary<int, TrackedRow> RowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<string, TrackedStore> StoreList { get; } = new Dictionary<string, TrackedStore>();
        public Dictionary<int, TrackedProcessInvocation> ProcessList { get; } = new Dictionary<int, TrackedProcessInvocation>();
        public Dictionary<string, Counter> Counters { get; } = new Dictionary<string, Counter>();

        public OnProcessAddedDelegate OnProcessInvoked { get; set; }
        public OnCountersUpdatedDelegate OnCountersUpdated { get; set; }
        public OnEventAddedDelegate OnEventsAdded { get; set; }

        public Playbook(ExecutionContext sessionContext)
        {
            ExecutionContext = sessionContext;
        }

        public void AddEvents(IEnumerable<AbstractEvent> abstactEvents)
        {
            var newEvents = new List<AbstractEvent>();

            ContextCountersUpdatedEvent lastContextCountersUpdatedEvent = null;
            foreach (var abstactEvent in abstactEvents)
            {
                switch (abstactEvent)
                {
                    case LogEvent evt:
                        {
                            if (evt.ProcessInvocationUID != null && !ProcessList.TryGetValue(evt.ProcessInvocationUID.Value, out var process))
                            {
                                continue;
                            }
                        }
                        break;
                    case DataStoreCommandEvent evt:
                        {
                            if (!ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                                continue;
                        }
                        break;
                    case ContextCountersUpdatedEvent evt:
                        lastContextCountersUpdatedEvent = evt;
                        break;
                    case ProcessInvocationStartEvent evt:
                        {
                            if (!ProcessList.TryGetValue(evt.InvocationUID, out var process))
                            {
                                TrackedProcessInvocation invoker = null;
                                if (evt.CallerInvocationUID != null && !ProcessList.TryGetValue(evt.CallerInvocationUID.Value, out invoker))
                                    continue;

                                process = new TrackedProcessInvocation(evt.InvocationUID, evt.InstanceUID, evt.InvocationCounter, invoker, evt.Type, evt.Kind, evt.Name, evt.Topic);
                                ProcessList.Add(process.InvocationUID, process);
                                OnProcessInvoked?.Invoke(this, process);
                            }
                        }
                        break;
                    case ProcessInvocationEndEvent evt:
                        {
                            if (!ProcessList.TryGetValue(evt.InvocationUID, out var process))
                                continue;

                            process.ElapsedMillisecondsAfterFinished = TimeSpan.FromMilliseconds(evt.ElapsedMilliseconds);
                        }
                        break;
                    case RowCreatedEvent evt:
                        {
                            if (!ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                                continue;

                            var row = new TrackedRow()
                            {
                                Uid = evt.RowUid,
                                CreatorProcess = process,
                            };

                            row.AllEvents.Add(evt);

                            foreach (var kvp in evt.Values)
                            {
                                if (kvp.Value != null)
                                {
                                    row.Values[kvp.Key] = kvp.Value;
                                }
                            }

                            RowList[row.Uid] = row;

                            process.CreateRow(row);
                        }
                        break;
                    case RowOwnerChangedEvent evt:
                        {
                            if (!ProcessList.TryGetValue(evt.PreviousProcessInvocationUID, out var previousProcess))
                                continue;

                            if (!RowList.TryGetValue(evt.RowUid, out var row))
                                continue;

                            TrackedProcessInvocation newProcess = null;
                            if (evt.NewProcessInvocationUID != null && !ProcessList.TryGetValue(evt.NewProcessInvocationUID.Value, out newProcess))
                                continue;

                            if (newProcess != null)
                            {
                                previousProcess.PassedRow(row, newProcess);
                                newProcess.InputRow(row, previousProcess);
                            }
                            else
                            {
                                row.DroppedByProcess = previousProcess;
                                previousProcess.DropRow(row);
                            }

                            row.AllEvents.Add(evt);
                        }
                        break;
                    case RowValueChangedEvent evt:
                        {
                            if (!RowList.TryGetValue(evt.RowUid, out var row))
                                continue;

                            if (evt.ProcessInvocationUID != null && !ProcessList.TryGetValue(evt.ProcessInvocationUID.Value, out var process))
                                continue;

                            row.AllEvents.Add(evt);

                            foreach (var kvp in evt.Values)
                            {
                                if (kvp.Value != null)
                                {
                                    row.Values[kvp.Key] = kvp.Value;
                                }
                                else
                                {
                                    row.Values.Remove(kvp.Key);
                                }
                            }
                        }
                        break;
                    case RowStoredEvent evt:
                        {
                            if (!RowList.TryGetValue(evt.RowUid, out var row))
                                continue;

                            if (!ProcessList.TryGetValue(evt.ProcessInvocationUID, out var process))
                                continue;

                            row.AllEvents.Add(evt);
                            var storePath = string.Intern(string.Join("/", evt.Locations.Select(x => x.Value)));
                            if (!StoreList.TryGetValue(storePath, out var store))
                            {
                                store = new TrackedStore(storePath);
                                StoreList.Add(storePath, store);
                            }

                            process.StoreRow(row, store);

                            var snapshot = row.GetSnapshot();
                            store.Rows.Add(new Tuple<RowStoredEvent, TrackedRowSnapshot>(evt, snapshot));
                        }
                        break;
                }

                newEvents.Add(abstactEvent);
            }

            if (newEvents.Count == 0)
                return;

            if (FirstEventTimestamp == null)
                FirstEventTimestamp = new DateTime(newEvents[0].Timestamp);

            LastEventTimestamp = new DateTime(newEvents[newEvents.Count - 1].Timestamp);

            OnEventsAdded?.Invoke(this, newEvents);

            if (lastContextCountersUpdatedEvent != null)
            {
                foreach (var counter in lastContextCountersUpdatedEvent.Counters)
                {
                    Counters[counter.Name] = counter;
                }

                OnCountersUpdated?.Invoke(this);
            }
        }
    }
}