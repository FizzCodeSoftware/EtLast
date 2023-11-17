namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ColumnRenameException : EtlException
{
    public ColumnRenameException(IProcess process)
        : base(process, "specified target column already exists")
    {
    }
}
