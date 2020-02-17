namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Uid}")]
    public class TrackedRow
    {
        public int Uid { get; set; }
        public TrackedProcessInvocation CreatorProcess { get; set; }

        // todo: possible memory issues
        //public List<AbstractRowEvent> AllEvents { get; } = new List<AbstractRowEvent>();

        // todo: possible memory issues
        public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();
    }
}