namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;
    using FizzCode.EtLast.Diagnostics.Interface;

    public class TrackedRowSnapshot
    {
        public int Uid { get; set; }
        public Dictionary<string, Argument> Values { get; } = new Dictionary<string, Argument>();
    }
}