namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;
    using System.Diagnostics;

    [DebuggerDisplay("{Row}")]
    public class TrackedRowSnapshot
    {
        public TrackedRow Row { get; set; }
        public Dictionary<string, Argument> Values { get; } = new Dictionary<string, Argument>();
    }
}