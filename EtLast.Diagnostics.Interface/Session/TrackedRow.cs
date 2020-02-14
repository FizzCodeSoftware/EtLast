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
        public TrackedProcessInvocation CurrentOwner { get; set; }

        // todo: possible memory issues
        public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();

        public TrackedRowSnapshot GetSnapshot()
        {
            return new TrackedRowSnapshot()
            {
                Row = this,
                Values = new List<KeyValuePair<string, object>>(Values),
            };
        }
    }
}