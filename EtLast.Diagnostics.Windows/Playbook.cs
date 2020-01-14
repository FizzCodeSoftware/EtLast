namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal class Playbook
    {
        public SessionContext Collection { get; }

        public List<object> Events { get; } = new List<object>();
        public Dictionary<int, TrackedRow> RowList { get; } = new Dictionary<int, TrackedRow>();
        public Dictionary<string, TrackedStore> StoreList { get; } = new Dictionary<string, TrackedStore>();
        public Dictionary<string, TrackedProcess> ProcessList { get; } = new Dictionary<string, TrackedProcess>();

        public Playbook(SessionContext collection)
        {
            Collection = collection;
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

                        if (!ProcessList.TryGetValue(evt.ProcessUid, out var process))
                        {
                            process = new TrackedProcess(evt.ProcessUid, evt.ProcessName);
                            ProcessList.Add(evt.ProcessUid, process);
                        }

                        process.AddRow(row);
                    }
                    break;
                case RowOwnerChangedEvent evt:
                    {
                        if (RowList.TryGetValue(evt.RowUid, out var row))
                        {
                            row.AllEvents.Add(evt);

                            if (!ProcessList.TryGetValue(evt.PreviousProcessUid, out var previousProcess))
                            {
                                previousProcess = new TrackedProcess(evt.PreviousProcessUid, evt.PreviousProcessName);
                                ProcessList.Add(evt.PreviousProcessUid, previousProcess);
                            }

                            if (!string.IsNullOrEmpty(evt.NewProcessUid))
                            {
                                if (!ProcessList.TryGetValue(evt.NewProcessUid, out var newProcess))
                                {
                                    newProcess = new TrackedProcess(evt.NewProcessUid, evt.NewProcessName);
                                    ProcessList.Add(evt.NewProcessUid, newProcess);
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