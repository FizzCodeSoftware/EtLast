namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class DuplicateKeyException : EtlException
    {
        public DuplicateKeyException(IProcess process, IReadOnlySlimRow row, string key)
            : base(process, "duplicate keys found")
        {
            Data.Add("Key", key);
            Data.Add("Row", row.ToDebugString());
        }
    }
}