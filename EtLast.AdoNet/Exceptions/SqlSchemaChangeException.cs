namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class SqlSchemaChangeException : EtlException
    {
        public SqlSchemaChangeException(IProcess process, string operation, Exception innerException)
            : base(process, "database schema change failed: " + operation)
        {
        }
    }
}