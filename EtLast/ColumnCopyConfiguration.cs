namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class ColumnCopyConfiguration
    {
        public string FromColumn { get; }
        public string ToColumn { get; }

        public ColumnCopyConfiguration(string fromColumn, string toColumn)
        {
            ToColumn = toColumn;
            FromColumn = fromColumn;
        }

        public ColumnCopyConfiguration(string fromColumn)
        {
            FromColumn = fromColumn;
            ToColumn = fromColumn;
        }

        public void Copy(IBaseOperation operation, IRow sourceRow, IRow targetRow)
        {
            var value = sourceRow[FromColumn];
            if (value != null)
            {
                targetRow.SetValue(ToColumn, value, operation);
            }
        }

        public void Copy(IRow sourceRow, List<KeyValuePair<string, object>> targetValues)
        {
            var value = sourceRow[FromColumn];
            if (value != null)
            {
                targetValues.Add(new KeyValuePair<string, object>(FromColumn, value));
            }
        }
    }
}