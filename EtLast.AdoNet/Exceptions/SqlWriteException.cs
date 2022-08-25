namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlWriteException : EtlException
{
    public SqlWriteException(IProcess process, Exception innerException)
        : base(process, "database write failed", innerException)
    {
    }
}
