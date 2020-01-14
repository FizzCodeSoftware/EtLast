﻿namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal class Playbook
    {
        public Collection Collection { get; }

        public List<object> AllEvents { get; } = new List<object>();

        public Dictionary<int, TrackedRow> AllRows { get; } = new Dictionary<int, TrackedRow>();

        public Playbook(Collection collection)
        {
            Collection = collection;
        }

        public void AddEvent(object payload)
        {
            AllEvents.Add(payload);

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

                        AllRows[row.Uid] = row;
                        break;
                    }
                case RowOwnerChangedEvent evt:
                    {
                        if (AllRows.TryGetValue(evt.RowUid, out var row))
                        {
                            row.AllEvents.Add(evt);
                            row.LastOwnerChangedEvent = evt;
                        }

                        break;
                    }

                case RowValueChangedEvent evt:
                    {
                        if (AllRows.TryGetValue(evt.RowUid, out var row))
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

                        break;
                    }
            }
        }
    }
}