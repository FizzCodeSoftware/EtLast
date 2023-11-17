namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlAggregateReadException(IProcess process, Exception innerException, string category)
    : EtlException(process, "database aggregate read failed: " + category, innerException)
{
}
