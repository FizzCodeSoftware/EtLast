namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchException : EtlException
{
    public TooManyMatchException(IProcess process, IReadOnlySlimRow row)
        : base(process, "too many match")
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public TooManyMatchException(IProcess process, IReadOnlySlimRow row, string key)
        : base(process, "too many match")
    {
        Data["Row"] = row.ToDebugString(true);
        Data["Key"] = key;
    }
}
