namespace FizzCode.EtLast
{
    public class ColumnCopyConfiguration
    {
        public string ToColumn { get; }
        public string FromColumn { get; }

        public ColumnCopyConfiguration(string toColumn, string fromColumn)
        {
            ToColumn = toColumn;
            FromColumn = fromColumn;
        }

        public ColumnCopyConfiguration(string copyFromColumn)
        {
            ToColumn = copyFromColumn;
            FromColumn = copyFromColumn;
        }
    }
}