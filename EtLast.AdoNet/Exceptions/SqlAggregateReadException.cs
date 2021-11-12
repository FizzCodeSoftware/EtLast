namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class SqlAggregateReadException : EtlException
    {
        public SqlAggregateReadException(IProcess process, Exception innerException, string category)
            : base(process, "database aggregate read failed: " + category)
        {
        }
    }
}