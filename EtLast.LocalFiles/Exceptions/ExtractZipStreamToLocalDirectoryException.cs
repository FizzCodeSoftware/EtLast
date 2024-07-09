namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ExtractZipStreamToLocalDirectoryException : EtlException
{
    internal ExtractZipStreamToLocalDirectoryException(IProcess process, Exception innerException)
        : base(process, "can't extract a zip stream to a local directory", innerException)
    {
    }
}