namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlReadException : EtlException
{
    public SqlReadException(IProcess process, Exception innerException)
        : base(process, "database read failed", innerException)
    {
    }
}
