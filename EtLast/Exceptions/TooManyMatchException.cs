namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class TooManyMatchException : EtlException
    {
        public TooManyMatchException(IProcess process, IReadOnlySlimRow row)
            : base(process, "too many match")
        {
            Data.Add("Row", row.ToDebugString());
        }

        public TooManyMatchException(IProcess process, IReadOnlySlimRow row, string key)
            : base(process, "too many match")
        {
            Data.Add("Row", row.ToDebugString());
            Data.Add("Key", key);
        }
    }
}