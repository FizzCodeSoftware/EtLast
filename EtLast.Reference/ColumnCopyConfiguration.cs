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

        public ColumnCopyConfiguration(string fromColumn)
        {
            ToColumn = fromColumn;
            FromColumn = fromColumn;
        }

        public void Copy(IBaseOperation operation, IRow sourceRow, IRow targetRow)
        {
            var value = sourceRow[FromColumn];
            if (value != null)
            {
                targetRow.SetValue(ToColumn, value, operation);
            }
        }
    }
}