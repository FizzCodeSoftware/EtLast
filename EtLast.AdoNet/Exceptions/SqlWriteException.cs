namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlWriteException(IProcess process, Exception innerException)
    : EtlException(process, "database write failed", innerException)
{
}
