namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DeleteDirectoryException : EtlException
{
    internal DeleteDirectoryException(IProcess process, Exception innerException)
        : base(process, "error during the deletion of a local directory", innerException)
    {
    }
}