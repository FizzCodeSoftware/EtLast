namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InitializerDelegateException(IProcess process, Exception innerException)
    : EtlException(process, "error during the initialization of the process", innerException)
{
}
