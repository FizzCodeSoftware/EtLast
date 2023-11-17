namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlStatementException(IProcess process, Exception innerException)
    : EtlException(process, "database SQL statement failed", innerException)
{
}
