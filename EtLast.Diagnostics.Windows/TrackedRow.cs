namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using FizzCode.EtLast.Diagnostics.Interface;

    [DebuggerDisplay("{Uid}")]
    public class TrackedRow
    {
        public int Uid { get; set; }
        public List<object> AllEvents { get; } = new List<object>();

        public RowCreatedEvent CreatedByEvent { get; set; }
        public TrackedProcess CurrentOwner { get; set; }

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