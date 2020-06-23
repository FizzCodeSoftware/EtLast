namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

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

        public void Copy(IReadOnlySlimRow sourceRow, List<KeyValuePair<string, object>> targetValues)
        {
            targetValues.Add(new KeyValuePair<string, object>(ToColumn, sourceRow[FromColumn]));
        }

        public static void CopyManyToRowStage(IReadOnlySlimRow sourceRow, IRow targetRow, List<ColumnCopyConfiguration> configurations)
        {
            foreach (var config in configurations)
            {
                targetRow.SetStagedValue(config.ToColumn, sourceRow[config.FromColumn]);
            }
        }

        public static void CopyManyToRow(IReadOnlySlimRow sourceRow, ISlimRow targetRow, List<ColumnCopyConfiguration> configurations)
        {
            foreach (var config in configurations)
            {
                targetRow.SetValue(config.ToColumn, sourceRow[config.FromColumn]);
            }
        }

        public static void CopyMany(IReadOnlySlimRow sourceRow, Dictionary<string, object> targetValues, List<ColumnCopyConfiguration> configurations)
        {
            foreach (var config in configurations)
            {
                targetValues[config.ToColumn] = sourceRow[config.FromColumn];
            }
        }

        public static List<ColumnCopyConfiguration> StraightCopy(params string[] columnNames)
        {
            return columnNames
                .Select(col => new ColumnCopyConfiguration(col))
                .ToList();
        }
    }
}