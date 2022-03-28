namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ProcessParameterNullException : InvalidProcessParameterException
{
    public ProcessParameterNullException(IProcess process, string parameterName)
        : base(process, parameterName, null, "value cannot be null or empty")
    {
    }
}
