namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidProcessParameterException : EtlException
{
    public InvalidProcessParameterException(IProcess process, string parameterName, object value, string cause)
        : base(process, "invalid parameter: " + parameterName + ", " + cause)
    {
        Data["Parameter"] = parameterName;
        Data["Value"] = value != null ? value.ToString() : "NULL";
        Data["Cause"] = cause;
    }
}
