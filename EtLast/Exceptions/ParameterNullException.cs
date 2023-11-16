namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ParameterNullException(string location, string parameterName) : InvalidParameterException(location, parameterName, null, "value cannot be null or empty")
{
}
