namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JsonArrayReaderException : EtlException
{
    public JsonArrayReaderException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}