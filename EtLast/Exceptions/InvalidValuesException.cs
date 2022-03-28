namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidValuesException : EtlException
{
    public InvalidValuesException(IProcess process, IReadOnlySlimRow row)
        : base(process, "invalid values found")
    {
        Data.Add("Row", row.ToDebugString(true));
    }
}
