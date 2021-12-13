namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class ColumnCopyConfiguration
    {
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public string SourceColumn { get; private set; }

        public ColumnCopyConfiguration FromSource(string sourceColumn)
        {
            SourceColumn = sourceColumn;
            return this;
        }

        public static Dictionary<string, ColumnCopyConfiguration> StraightCopyAllColumn(params string[] columnNames)
        {
            return columnNames
                .ToDictionary(x => x, x => new ColumnCopyConfiguration());
        }
    }
}