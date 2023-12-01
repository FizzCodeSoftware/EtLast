namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ColumnRenameException(IProcess process) : EtlException(process, "specified target column already exists")
{
}
