namespace FizzCode.EtLast.DwhBuilder;

[ComVisible(true)]
[Serializable]
public class InvalidDwhBuilderParameterException<TTableBuilder> : EtlException
    where TTableBuilder : IDwhTableBuilder
{
    public InvalidDwhBuilderParameterException(IDwhBuilder<TTableBuilder> builder, string parameterName, object value, string cause)
        : base("invalid DWH builder parameter")
    {
        Data.Add("Builder", builder.ScopeName);
        Data.Add("Parameter", parameterName);
        Data.Add("Value", value != null ? value.ToString() : "NULL");
        Data.Add("Cause", cause);
    }
}
