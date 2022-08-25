namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlMergeException : EtlException
{
    public SqlMergeException(IProcess process, Exception innerException)
        : base(process, "database merge failed", innerException)
    {
    }
}
