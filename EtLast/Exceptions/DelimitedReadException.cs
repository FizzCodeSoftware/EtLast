namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DelimitedReadException : EtlException
{
    public DelimitedReadException(IProcess process, string message, NamedStream stream)
        : base(process, message)
    {
        Data["StreamName"] = stream.Name;
    }

    public DelimitedReadException(IProcess process, string message, NamedStream stream, Exception innerException)
        : base(process, message, innerException)
    {
        Data["StreamName"] = stream.Name;
    }
}