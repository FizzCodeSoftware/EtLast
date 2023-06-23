namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class HttpDownloadToLocalFileException : EtlException
{
    internal HttpDownloadToLocalFileException(IProcess process, string message)
        : base(process, message)
    {
    }

    internal HttpDownloadToLocalFileException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
