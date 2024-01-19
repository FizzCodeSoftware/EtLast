namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JsonSerializerException(IProcess process, string message, Exception innerException)
    : EtlException(process, message, innerException)
{
}