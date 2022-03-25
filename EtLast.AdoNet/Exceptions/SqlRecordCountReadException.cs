namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlRecordCountReadException : EtlException
{
    public SqlRecordCountReadException(IProcess process, Exception innerException)
        : base(process, "database table record count query failed")
    {
    }
}
