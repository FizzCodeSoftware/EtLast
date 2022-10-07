namespace FizzCode.EtLast.DwhBuilder;

[ComVisible(true)]
[Serializable]
public class InvalidDwhBuilderParameterException<TTableBuilder> : EtlException
    where TTableBuilder : IDwhTableBuilder
{
    public InvalidDwhBuilderParameterException(IDwhBuilder<TTableBuilder> builder, string parameterName, object value, string cause)
        : base("invalid DWH builder parameter")
    {
        Data["Builder"] = builder.ScopeName;
        Data["Parameter"] = parameterName;
        Data["Value"] = value != null ? value.ToString() : "NULL";
        Data["Cause"] = cause;
    }
}
