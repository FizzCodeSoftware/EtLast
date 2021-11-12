namespace FizzCode.EtLast
{
    public sealed class ColumnRenameConfiguration
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