namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlTruncateException(IProcess process, Exception innerException)
    : EtlException(process, "database truncate failed", innerException)
{
}
