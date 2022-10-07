namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DuplicateKeyException : EtlException
{
    public DuplicateKeyException(IProcess process, IReadOnlySlimRow row, string key)
        : base(process, "duplicate keys found")
    {
        Data["Key"] = key;
        Data["Row"] = row.ToDebugString(true);
    }
}
