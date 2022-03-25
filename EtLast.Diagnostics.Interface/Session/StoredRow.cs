namespace FizzCode.EtLast.Diagnostics.Interface;

using System.Collections.Generic;
using System.Diagnostics;

[DebuggerDisplay("{Row}")]
public class StoredRow
{
    public int Uid { get; set; }
    public TrackedSink Store { get; set; }
    public TrackedProcessInvocation Process { get; set; }
    public KeyValuePair<string, object>[] Values { get; set; }
}
