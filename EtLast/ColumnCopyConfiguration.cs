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

        public void Copy(IRow sourceRow, List<KeyValuePair<string, object>> targetValues)
        {
            targetValues.Add(new KeyValuePair<string, object>(ToColumn, sourceRow[FromColumn]));
        }

        public static void CopyManyToRowStage(IRow sourceRow, IRow targetRow, List<ColumnCopyConfiguration> configurations)
        {
            foreach (var config in configurations)
            {
                targetRow.Staging[config.ToColumn] = sourceRow[config.FromColumn];
            }
        }

        public static void CopyMany(IRow sourceRow, Dictionary<string, object> targetValues, List<ColumnCopyConfiguration> configurations)
        {
            foreach (var x in configurations)
            {
                targetValues[x.ToColumn] = sourceRow[x.FromColumn];
            }
        }
    }
}