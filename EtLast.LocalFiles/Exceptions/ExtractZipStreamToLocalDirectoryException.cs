namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ExtractZipStreamToLocalDirectoryException : EtlException
{
    internal ExtractZipStreamToLocalDirectoryException(IProcess process, Exception innerException)
        : base(process, "error during the extraction of a zip stream to a local directory", innerException)
    {
    }
}