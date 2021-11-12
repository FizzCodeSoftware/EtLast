namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class SqlIdentityResetException : EtlException
    {
        public SqlIdentityResetException(IProcess process, Exception innerException)
            : base(process, "database identity counter reset failed")
        {
        }
    }
}