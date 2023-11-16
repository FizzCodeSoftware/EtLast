namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlRecordCountReadException(IProcess process, Exception innerException) : EtlException(process, "database table record count query failed", innerException)
{
}
