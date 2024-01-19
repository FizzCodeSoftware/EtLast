namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JsonDeserializerException(IProcess process, string message, Exception innerException)
    : EtlException(process, message, innerException)
{
}