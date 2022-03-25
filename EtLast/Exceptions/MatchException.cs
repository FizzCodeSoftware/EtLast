namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class MatchException : EtlException
{
    public MatchException(IProcess process, IReadOnlySlimRow row)
        : base(process, "match")
    {
        Data.Add("Row", row.ToDebugString(true));
    }

    public MatchException(IProcess process, IReadOnlySlimRow row, string key)
        : base(process, "match")
    {
        Data.Add("Row", row.ToDebugString(true));
        Data.Add("Key", key);
    }
}
