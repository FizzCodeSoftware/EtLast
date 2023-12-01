namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DuplicateKeyException(IProcess process) : EtlException(process, "duplicate keys found")
{
}
