namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlSchemaChangeException(IProcess process, string operation, Exception innerException)
    : EtlException(process, "database schema change failed: " + operation, innerException)
{
}
