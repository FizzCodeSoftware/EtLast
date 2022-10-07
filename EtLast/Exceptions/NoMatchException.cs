namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class NoMatchException : EtlException
{
    public NoMatchException(IProcess process, IReadOnlySlimRow row)
        : base(process, "no match")
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public NoMatchException(IProcess process, IReadOnlySlimRow row, string key)
        : base(process, "no match")
    {
        Data["Row"] = row.ToDebugString(true);
        Data["Key"] = key;
    }
}
