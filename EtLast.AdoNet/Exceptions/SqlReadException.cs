namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlReadException : EtlException
{
    public SqlReadException(IProcess process, Exception innerException)
        : base(process, "database read failed")
    {
    }
}
