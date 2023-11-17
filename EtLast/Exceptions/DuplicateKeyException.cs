namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DuplicateKeyException : EtlException
{
    public DuplicateKeyException(IProcess process)
        : base(process, "duplicate keys found")
    {
    }
}
