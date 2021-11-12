namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class SqlConnectionException : EtlException
    {
        public SqlConnectionException(IProcess process, Exception innerException)
            : base(process, "database connection failed")
        {
        }
    }
}