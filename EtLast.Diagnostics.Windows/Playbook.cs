namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal class Playbook
    {
        public Collection Collection { get; }

        public object[] AllEvents = Array.Empty<object>();
        private int _firstEventIndex;

        public Dictionary<int, TrackedRow> AllRows { get; } = new Dictionary<int, TrackedRow>();

        public Playbook(Collection collection)
        {
            Collection = collection;
        }

        public void AddEvent(int num, object payload)
        {
            if (AllEvents.Length == 0)
            {
                _firstEventIndex = num;
            }

            var count = num - _firstEventIndex + 1;
            Array.Resize(ref AllEvents, count);
            AllEvents[count - 1] = payload;

            switch (payload)
            {
                case RowCreatedEvent rowCreatedEvent:
                    {
                        var row = new TrackedRow()
                        {
                            Uid = rowCreatedEvent.RowUid,
                            CreatedByEvent = rowCreatedEvent,
                        };

                        row.AllEvents.Add(rowCreatedEvent);

                        AllRows[row.Uid] = row;
                        break;
                    }
                case RowOwnerChangedEvent rowOwnerChangedEvent:
                    {
                        var row = AllRows[rowOwnerChangedEvent.RowUid];
                        row.AllEvents.Add(payload);
                        row.LastOwnerChangedEvent = rowOwnerChangedEvent;
                        break;
                    }
            }
        }
    }
}