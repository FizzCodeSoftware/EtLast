namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlSchemaReadException : EtlException
{
    public SqlSchemaReadException(IProcess process, string category, Exception innerException)
        : base(process, "database schema read failed: " + category)
    {
    }
}
