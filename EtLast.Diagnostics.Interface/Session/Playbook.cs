namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public delegate void OnProcessAddedDelegate(Playbook playbook, TrackedProcess process);
    public delegate void OnCountersUpdatedDelegate(Playbook playbook);

    public class Playbook
    {
        public SessionContext Context { get; }

        public List<object> Events { get; } = new List<object>();
        public Dictionary<int, TrackedRow> RowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<string, TrackedStore> StoreList { get; } = new Dictionary<string, TrackedStore>();
        public Dictionary<int, TrackedProcess> ProcessList { get; } = new Dictionary<int, TrackedProcess>();
        public List<Counter> Counters { get; } = new List<Counter>();

        public OnProcessAddedDelegate OnProcessAdded { get; set; }
        public OnCountersUpdatedDelegate OnCountersUpdated { get; set; }

        public Playbook(SessionContext sessionContext)
        {
            Context = sessionContext;
        }

        public bool AddEvent(object payload)
        {
            Events.Add(payload);

            switch (payload)
            {
                case LogEvent evt:
                    {
                        if (evt.ProcessUid != null && !ProcessList.TryGetValue(evt.ProcessUid.Value, out var process))
                            return false;
                    }
                    break;
                case ContextCountersUpdatedEvent evt:
                    {
                        Counters.Clear();
                        Counters.AddRange(evt.Counters);
                        OnCountersUpdated?.Invoke(this);
                    }
                    break;
                case ProcessCreatedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.Uid, out var process))
                        {
                            process = new TrackedProcess(evt.Uid, evt.Type, evt.Name);
                            ProcessList.Add(process.Uid, process);
                            OnProcessAdded?.Invoke(this, process);
                        }
                    }
                    break;
                case RowCreatedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.ProcessUid, out var process))
                            return false;

                        var row = new TrackedRow()
                        {
                            Uid = evt.RowUid,
                            CreatedByEvent = evt,
                        };

                        row.AllEvents.Add(evt);

                        foreach (var value in evt.Values)
                        {
                            if (value.Value != null)
                            {
                                row.Values[value.Name] = value;
                            }
                        }

                        RowList[row.Uid] = row;

                        process.AddRow(row);
                    }
                    break;
                case RowOwnerChangedEvent evt:
                    {
                        if (!ProcessList.TryGetValue(evt.PreviousProcessUid, out var previousProcess))
                            return false;

                        if (!RowList.TryGetValue(evt.RowUid, out var row))
                            return false;

                        TrackedProcess newProcess = null;
                        if (evt.NewProcessUid != null && !ProcessList.TryGetValue(evt.NewProcessUid.Value, out newProcess))
                            return false;

                        if (newProcess != null)
                        {
                            previousProcess.RemoveRow(row);
                            newProcess.AddRow(row);
                        }
                        else
                        {
                            previousProcess.DropRow(row);
                        }

                        row.AllEvents.Add(evt);
                    }
                    break;
                case RowValueChangedEvent evt:
                    {
                        if (!RowList.TryGetValue(evt.RowUid, out var row))
                            return false;

                        if (evt.ProcessUid != null && !ProcessList.TryGetValue(evt.ProcessUid.Value, out var process))
                            return false;

                        row.AllEvents.Add(evt);
                        if (evt.CurrentValue != null)
                        {
                            row.Values[evt.Column] = evt.CurrentValue;
                        }
                        else
                        {
                            row.Values.Remove(evt.Column);
                        }
                    }
                    break;
                case RowStoredEvent evt:
                    {
                        if (!RowList.TryGetValue(evt.RowUid, out var row))
                            return false;

                        if (!ProcessList.TryGetValue(evt.ProcessUid, out var process))
                            return false;

                        row.AllEvents.Add(evt);
                        var storePath = string.Join("/", evt.Locations.Select(x => x.Value));
                        if (!StoreList.TryGetValue(storePath, out var store))
                        {
                            store = new TrackedStore(storePath);
                            StoreList.Add(storePath, store);
                        }

                        var snapshot = row.GetSnapshot();
                        store.Rows.Add(new Tuple<RowStoredEvent, TrackedRowSnapshot>(evt, snapshot));
                    }
                    break;
            }

            return true;
        }
    }
}