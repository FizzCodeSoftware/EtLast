namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class HttpDownloadToLocalFileException : EtlException
{
    internal HttpDownloadToLocalFileException(IProcess process, string message, string url, string fileName)
        : base(process, message)
    {
        Data.Add("Url", url);
        Data.Add("FileName", fileName);
    }

    internal HttpDownloadToLocalFileException(IProcess process, string message, string url, string fileName, Exception innerException)
        : base(process, message, innerException)
    {
        Data.Add("Url", url);
        Data.Add("FileName", fileName);
    }
}
