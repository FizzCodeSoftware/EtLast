namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class HttpDownloadException : EtlException
{
    internal HttpDownloadException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
