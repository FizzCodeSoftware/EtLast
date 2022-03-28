namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlRecordCountReadException : EtlException
{
    public SqlRecordCountReadException(IProcess process, Exception innerException)
        : base(process, "database table record count query failed")
    {
    }
}
