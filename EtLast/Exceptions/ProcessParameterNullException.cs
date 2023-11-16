namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ProcessParameterNullException(IProcess process, string parameterName) : InvalidProcessParameterException(process, parameterName, null, "value cannot be null or empty")
{
}
