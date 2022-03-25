namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlTruncateException : EtlException
{
    public SqlTruncateException(IProcess process, Exception innerException)
        : base(process, "database truncate failed")
    {
    }
}
