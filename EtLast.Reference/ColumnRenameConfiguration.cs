namespace FizzCode.EtLast
{
    public class ColumnRenameConfiguration
    {
        public string CurrentName { get; }
        public string NewName { get; }

        public ColumnRenameConfiguration(string currentName, string newName)
        {
            CurrentName = currentName;
            NewName = newName;
        }
    }
}