namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class LocalFileDeleteException : EtlException
{
    internal LocalFileDeleteException(IProcess process, string message, string fileName)
        : base(process, message)
    {
        Data.Add("FileName", fileName);
    }

    internal LocalFileDeleteException(IProcess process, string message, string fileName, Exception innerException)
        : base(process, message, innerException)
    {
        Data.Add("FileName", fileName);
    }
}
