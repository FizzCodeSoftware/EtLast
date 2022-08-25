namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlConnectionException : EtlException
{
    public SqlConnectionException(IProcess process, Exception innerException)
        : base(process, "database connection failed", innerException)
    {
    }
}
