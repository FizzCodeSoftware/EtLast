namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class SqlWriteException : EtlException
    {
        public SqlWriteException(IProcess process, Exception innerException)
            : base(process, "database write failed")
        {
        }
    }
}