namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlSchemaReadException : EtlException
{
    public SqlSchemaReadException(IProcess process, string category, Exception innerException)
        : base(process, "database schema read failed: " + category, innerException)
    {
    }
}
