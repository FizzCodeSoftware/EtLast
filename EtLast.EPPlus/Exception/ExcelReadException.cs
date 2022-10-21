﻿namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ExcelReadException : EtlException
{
    public ExcelReadException(IProcess process, string message)
        : base(process, message)
    {
    }

    public ExcelReadException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
