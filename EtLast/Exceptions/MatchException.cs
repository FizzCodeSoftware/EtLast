namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class MatchException : EtlException
    {
        public MatchException(IProcess process, IReadOnlySlimRow row)
            : base(process, "match")
        {
            Data.Add("Row", row.ToDebugString());
        }

        public MatchException(IProcess process, IReadOnlySlimRow row, string key)
            : base(process, "match")
        {
            Data.Add("Row", row.ToDebugString());
            Data.Add("Key", key);
        }
    }
}