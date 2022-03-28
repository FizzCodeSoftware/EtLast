namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidParameterException : EtlException
{
    public InvalidParameterException(string location, string parameterName, object value, string cause)
        : base("invalid parameter")
    {
        Data.Add("Location", location);
        Data.Add("Parameter", parameterName);
        Data.Add("Value", value != null ? value.ToString() : "NULL");
        Data.Add("Cause", cause);
    }
}
