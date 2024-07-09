namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DeleteDirectoryException : EtlException
{
    internal DeleteDirectoryException(IProcess process, Exception innerException)
        : base(process, "can't delete local directory", innerException)
    {
    }
}