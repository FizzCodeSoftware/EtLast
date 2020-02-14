namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    [DebuggerDisplay("{Uid}")]
    public class TrackedRow
    {
        public int Uid { get; set; }
        public TrackedProcessInvocation CreatorProcess { get; set; }
        public TrackedProcessInvocation DroppedByProcess { get; set; }
        public TrackedProcessInvocation CurrentProcess { get; set; }

        // todo: possible memory issues
        public List<AbstractRowEvent> AllEvents { get; } = new List<AbstractRowEvent>();

        // todo: possible memory issues
        public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();

        public TrackedRowSnapshot GetSnapshot()
        {
            return new TrackedRowSnapshot()
            {
                Row = this,
                Values = Values.ToArray(),
            };
        }
    }
}