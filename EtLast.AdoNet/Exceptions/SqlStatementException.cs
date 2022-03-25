namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class SqlStatementException : EtlException
{
    public SqlStatementException(IProcess process, Exception innerException)
        : base(process, "database SQL statement failed")
    {
    }
}
