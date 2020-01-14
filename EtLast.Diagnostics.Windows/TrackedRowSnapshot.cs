namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using FizzCode.EtLast.Diagnostics.Interface;

    [DebuggerDisplay("{Row}")]
    public class TrackedRowSnapshot
    {
        public TrackedRow Row { get; set; }
        public Dictionary<string, Argument> Values { get; } = new Dictionary<string, Argument>();
    }
}