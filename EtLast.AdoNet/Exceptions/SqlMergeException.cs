namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlMergeException(IProcess process, Exception innerException) : EtlException(process, "database merge failed", innerException)
{
}
