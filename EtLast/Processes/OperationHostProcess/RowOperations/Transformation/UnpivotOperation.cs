namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    /// <summary>
    /// Id, Name, Cars, Houses, Kids
    /// 1, A, 1, 1, 2
    /// 2, B, 2, 1, 3
    /// FixColumns: Id, Name
    /// NewColumnForDimension: InventoryItem (values will contains non-fix column names: Cars, Houses, Kids)
    /// NewColumnForValue: Amount (values will contain values in cells: 1, 2, 3)
    /// result:
    /// Id, Name, InventoryItem, Amount
    /// 1, A, Cars, 1
    /// 1, A, Houses, 1
    /// 1, A, Kids, 2
    /// 2, B, Cars, 2
    /// 2, B, Houses, 1
    /// 2, B, Kids, 3
    /// </summary>
    public class UnpivotOperation : AbstractRowOperation
    {
        public string[] FixColumns { get; set; }
        public string NewColumnForDimension { get; set; }
        public string NewColumnForValue { get; set; }
        public bool IgnoreIfValueIsNull { get; set; } = true;
        private HashSet<string> _fixColumns;

        public override void Apply(IRow row)
        {
            foreach (var cell in row.Values)
            {
                if (cell.Value == null && IgnoreIfValueIsNull)
                    continue;

                if (_fixColumns?.Contains(cell.Key) == true)
                    continue;

                var newRow = Process.Context.CreateRow(FixColumns.Length + 2);
                newRow.CurrentOperation = this;

                foreach (var column in FixColumns)
                {
                    newRow.SetValue(column, row[column], this);
                }

                newRow.SetValue(NewColumnForDimension, cell.Key, this);
                newRow.SetValue(NewColumnForValue, cell.Value, this);

                Process.AddRow(newRow, this);
            }

            Process.RemoveRow(row, this);
        }

        public override void Prepare()
        {
            if (FixColumns == null)
                throw new OperationParameterNullException(this, nameof(FixColumns));

            if (NewColumnForValue == null)
                throw new OperationParameterNullException(this, nameof(NewColumnForValue));

            if (NewColumnForDimension == null)
                throw new OperationParameterNullException(this, nameof(NewColumnForDimension));

            if (FixColumns.Length > 0)
            {
                _fixColumns = new HashSet<string>(FixColumns);
            }
        }
    }
}