namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlSchemaReadException(IProcess process, string category, Exception innerException) : EtlException(process, "database schema read failed: " + category, innerException)
{
}
