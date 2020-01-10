namespace FizzCode.EtLast.Debugger.Windows
{
    using System;
    using System.Collections.Generic;

    internal class Collection
    {
        public string Name { get; set; }

        public Collection ParentCollection { get; set; }
        public List<Collection> ChildCollections { get; } = new List<Collection>();
        public Dictionary<string, Collection> ChildCollectionsByName { get; } = new Dictionary<string, Collection>();

        public object[] AllEvents = Array.Empty<object>();
        private int _firstEventIndex;

        public Dictionary<int, TrackedRow> AllRows { get; } = new Dictionary<int, TrackedRow>();

        public void AddEvent(int num, object payload)
        {
            if (AllEvents.Length == 0)
            {
                _firstEventIndex = num;
            }

            var count = num - _firstEventIndex + 1;
            Array.Resize(ref AllEvents, count);
            AllEvents[count - 1] = payload;
        }
    }
}