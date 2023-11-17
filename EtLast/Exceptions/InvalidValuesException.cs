namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidValuesException : EtlException
{
    public InvalidValuesException(IProcess process)
        : base(process, "invalid values found")
    {
    }
}
