namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class RowContainsErrorException : EtlException
    {
        public RowContainsErrorException(IProcess process, IReadOnlySlimRow row)
            : base(process, "error found in a row")
        {
            Data.Add("Row", row.ToDebugString(true));
        }
    }
}