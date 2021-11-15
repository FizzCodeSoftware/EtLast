namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ColumnRenameException : EtlException
    {
        public ColumnRenameException(IProcess process, IReadOnlySlimRow row, string currentName, string newName)
            : base(process, "specified target column already exists")
        {
            Data.Add("CurrentName", currentName);
            Data.Add("NewName", newName);
            Data.Add("Row", row.ToDebugString(true));
        }
    }
}