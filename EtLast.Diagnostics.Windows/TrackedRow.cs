﻿namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;
    using FizzCode.EtLast.Diagnostics.Interface;

    public class TrackedRow
    {
        public int Uid { get; set; }
        public List<object> AllEvents { get; } = new List<object>();

        public RowCreatedEvent CreatedByEvent { get; set; }
        public RowOwnerChangedEvent LastOwnerChangedEvent { get; set; }

        public Dictionary<string, Argument> Values { get; } = new Dictionary<string, Argument>();

        public TrackedRowSnapshot GetSnapshot()
        {
            var snapshot = new TrackedRowSnapshot()
            {
                Uid = Uid,
            };

            foreach (var kvp in Values)
            {
                snapshot.Values[kvp.Key] = kvp.Value;
            }

            return snapshot;
        }
    }
}