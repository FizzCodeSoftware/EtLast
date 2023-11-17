namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlConnectionException(IProcess process, Exception innerException)
    : EtlException(process, "database connection failed", innerException)
{
}
