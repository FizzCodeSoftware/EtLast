namespace FizzCode.EtLast.DwhBuilder;

[ComVisible(true)]
[Serializable]
public class DwhBuilderParameterNullException<TTableBuilder>(IDwhBuilder<TTableBuilder> builder, string parameterName) : InvalidDwhBuilderParameterException<TTableBuilder>(builder, parameterName, null, "value cannot be null or empty")
    where TTableBuilder : IDwhTableBuilder
{
}
