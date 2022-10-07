namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class StreamReadException : EtlException
{
    public StreamReadException(IProcess process, string message, NamedStream stream)
        : base(process, message)
    {
        Data["StreamName"] = stream.Name;
    }

    public StreamReadException(IProcess process, string message, NamedStream stream, Exception innerException)
        : base(process, message, innerException)
    {
        Data["StreamName"] = stream.Name;
    }
}