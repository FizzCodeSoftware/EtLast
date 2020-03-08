namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Uid}")]
    public class TrackedRow
    {
        public int Uid { get; set; }
        public TrackedProcessInvocation CreatorProcess { get; set; }
        public TrackedProcessInvocation PreviousProcess { get; set; }
        public TrackedProcessInvocation NextProcess { get; set; }

        public List<AbstractRowEvent> AllEvents { get; } = new List<AbstractRowEvent>();

        public Dictionary<string, object> Values { get; set; }
    }
}