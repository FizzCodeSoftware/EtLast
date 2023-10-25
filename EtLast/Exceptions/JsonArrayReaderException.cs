namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JsonArrayReaderException : EtlException
{
    public JsonArrayReaderException(IProcess process, string message, NamedStream stream)
        : base(process, message)
    {
        Data["StreamName"] = stream.Name;
    }

    public JsonArrayReaderException(IProcess process, string message, NamedStream stream, Exception innerException)
        : base(process, message, innerException)
    {
        Data["StreamName"] = stream.Name;
    }
}