namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class LocalFileDeleteException : EtlException
{
    internal LocalFileDeleteException(IProcess process, string message)
        : base(process, message)
    {
    }

    internal LocalFileDeleteException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
