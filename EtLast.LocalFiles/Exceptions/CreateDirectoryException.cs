namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class CreateDirectoryException : EtlException
{
    internal CreateDirectoryException(IProcess process, Exception innerException)
        : base(process, "can't create local directory", innerException)
    {
    }
}