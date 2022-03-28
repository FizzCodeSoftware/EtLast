namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlDeleteException : EtlException
{
    public SqlDeleteException(IProcess process, Exception innerException)
        : base(process, "database delete failed")
    {
    }
}
