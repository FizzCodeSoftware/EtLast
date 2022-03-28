namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class CustomCodeException : EtlException
{
    public CustomCodeException(IProcess process, string message, Exception innerException)
        : base(process, message, innerException)
    {
    }
}
