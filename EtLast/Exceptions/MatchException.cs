namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class MatchException : EtlException
{
    public MatchException(IProcess process, IReadOnlySlimRow row)
        : base(process, "match")
    {
        Data["Row"] = row.ToDebugString(true);
    }

    public MatchException(IProcess process, IReadOnlySlimRow row, string key)
        : base(process, "match")
    {
        Data["Row"] = row.ToDebugString(true);
        Data["Key"] = key;
    }
}
