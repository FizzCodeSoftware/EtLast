namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class NoMatchException : EtlException
    {
        public NoMatchException(IProcess process, IReadOnlySlimRow row)
            : base(process, "no match")
        {
            Data.Add("Row", row.ToDebugString());
        }

        public NoMatchException(IProcess process, IReadOnlySlimRow row, string key)
            : base(process, "no match")
        {
            Data.Add("Row", row.ToDebugString());
            Data.Add("Key", key);
        }
    }
}