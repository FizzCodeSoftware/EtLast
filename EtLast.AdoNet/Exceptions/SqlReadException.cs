namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlReadException(IProcess process, Exception innerException) : EtlException(process, "database read failed", innerException)
{
}
