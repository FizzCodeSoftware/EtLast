namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlStatementException : EtlException
{
    public SqlStatementException(IProcess process, Exception innerException)
        : base(process, "database SQL statement failed", innerException)
    {
    }
}
