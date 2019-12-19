namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public delegate void OnProcessAddedDelegate(Playbook playbook, TrackedProcess process);

    public class Playbook
    {
        public SessionContext Context { get; }

        public List<object> Events { get; } = new List<object>();
        public Dictionary<int, TrackedRow> RowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<string, TrackedStore> StoreList { get; } = new Dictionary<string, TrackedStore>();
        public Dictionary<string, TrackedProcess> ProcessList { get; } = new Dictionary<string, TrackedProcess>();
        public OnProcessAddedDelegate OnProcessAdded { get; set; }

        public Playbook(SessionContext sessionContext)
        {
            Context = sessionContext;
        }

        public void AddEvent(object payload)
        {
            Events.Add(payload);

            switch (payload)
            {
                case RowCreatedEvent evt:
                    {
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

                        if (!ProcessList.TryGetValue(evt.Process.Uid, out var process))
                        {
                            process = new TrackedProcess(evt.Process);
                            ProcessList.Add(evt.Process.Uid, process);
                            OnProcessAdded?.Invoke(this, process);
                        }

                        process.AddRow(row);
                    }
                    break;
                case RowOwnerChangedEvent evt:
                    {
                        if (RowList.TryGetValue(evt.RowUid, out var row))
                        {
                            row.AllEvents.Add(evt);

                            if (!ProcessList.TryGetValue(evt.PreviousProcess.Uid, out var previousProcess))
                            {
                                previousProcess = new TrackedProcess(evt.PreviousProcess);
                                ProcessList.Add(evt.PreviousProcess.Uid, previousProcess);
                                OnProcessAdded?.Invoke(this, previousProcess);
                            }

                            if (evt.NewProcess != null)
                            {
                                if (!ProcessList.TryGetValue(evt.NewProcess.Uid, out var newProcess))
                                {
                                    newProcess = new TrackedProcess(evt.NewProcess);
                                    ProcessList.Add(evt.NewProcess.Uid, newProcess);
                                    OnProcessAdded?.Invoke(this, newProcess);
                                }

                                previousProcess.RemoveRow(row);
                                newProcess.AddRow(row);
                            }
                            else
                            {
                                previousProcess.DropRow(row);
                            }
                        }
                    }
                    break;
                case RowValueChangedEvent evt:
                    {
                        if (RowList.TryGetValue(evt.RowUid, out var row))
                        {
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
                    }
                    break;
                case RowStoredEvent evt:
                    {
                        if (RowList.TryGetValue(evt.RowUid, out var row))
                        {
                            if (!ProcessList.TryGetValue(evt.Process.Uid, out var process))
                            {
                                process = new TrackedProcess(evt.Process);
                                ProcessList.Add(evt.Process.Uid, process);
                                OnProcessAdded?.Invoke(this, process);
                            }

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
                    }
                    break;
            }
        }
    }
}