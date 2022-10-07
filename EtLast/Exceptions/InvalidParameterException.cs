namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidParameterException : EtlException
{
    public InvalidParameterException(string location, string parameterName, object value, string cause)
        : base("invalid parameter")
    {
        Data["Location"] = location;
        Data["Parameter"] = parameterName;
        Data["Value"] = value != null ? value.ToString() : "NULL";
        Data["Cause"] = cause;
    }
}
