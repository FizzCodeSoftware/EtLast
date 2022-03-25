namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlDeleteException : EtlException
{
    public SqlDeleteException(IProcess process, Exception innerException)
        : base(process, "database delete failed")
    {
    }
}
