namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ParameterNullException : InvalidParameterException
{
    public ParameterNullException(string location, string parameterName)
        : base(location, parameterName, null, "value cannot be null or empty")
    {
    }
}
