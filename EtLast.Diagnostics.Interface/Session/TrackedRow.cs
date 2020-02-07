namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Uid}")]
    public class TrackedRow
    {
        public int Uid { get; set; }

        // todo: possible memory issues
        public List<AbstractEvent> AllEvents { get; } = new List<AbstractEvent>();

        public RowCreatedEvent CreatedByEvent { get; set; }
        public RowOwnerChangedEvent DroppedByEvent { get; set; }
        public TrackedProcess CurrentOwner { get; set; }

        // todo: possible memory issues
        public Dictionary<string, Argument> Values { get; } = new Dictionary<string, Argument>();

        public TrackedRowSnapshot GetSnapshot()
        {
            var snapshot = new TrackedRowSnapshot()
            {
                Row = this,
            };

            foreach (var kvp in Values)
            {
                snapshot.Values[kvp.Key] = kvp.Value;
            }

            return snapshot;
        }
    }
}