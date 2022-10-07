namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class ColumnRenameException : EtlException
{
    public ColumnRenameException(IProcess process, IReadOnlySlimRow row, string currentName, string newName)
        : base(process, "specified target column already exists")
    {
        Data["CurrentName"] = currentName;
        Data["NewName"] = newName;
        Data["Row"] = row.ToDebugString(true);
    }
}
