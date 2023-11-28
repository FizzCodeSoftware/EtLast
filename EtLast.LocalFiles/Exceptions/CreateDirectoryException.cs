namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class CreateDirectoryException : EtlException
{
    internal CreateDirectoryException(IProcess process, Exception innerException)
        : base(process, "error during the creation of a local directory", innerException)
    {
    }
}