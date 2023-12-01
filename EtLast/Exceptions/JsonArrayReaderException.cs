namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JsonArrayReaderException(IProcess process, string message, Exception innerException) : EtlException(process, message, innerException)
{
}