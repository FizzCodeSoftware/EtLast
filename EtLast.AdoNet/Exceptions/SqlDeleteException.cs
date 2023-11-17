namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlDeleteException(IProcess process, Exception innerException)
    : EtlException(process, "database delete failed", innerException)
{
}
