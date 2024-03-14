namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class LocalFileReadException : EtlException
{
    internal LocalFileReadException(IProcess process, string message)
        : base(process, message)
    {
    }

    internal LocalFileReadException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
