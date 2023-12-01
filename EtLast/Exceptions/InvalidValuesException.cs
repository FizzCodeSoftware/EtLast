namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidValuesException(IProcess process) : EtlException(process, "invalid values found")
{
}
