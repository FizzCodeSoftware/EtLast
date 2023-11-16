namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class CustomCodeException(IProcess process, string message, Exception innerException) : EtlException(process, message, innerException)
{
}
