namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class StreamReadException : EtlException
{
    public StreamReadException(IProcess process, string message)
        : base(process, message)
    {
    }

    public StreamReadException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}