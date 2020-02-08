namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

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
    public class UnpivotMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string[] FixColumns { get; set; }
        public string NewColumnForDimension { get; set; }
        public string NewColumnForValue { get; set; }
        public bool IgnoreIfValueIsNull { get; set; } = true;

        public UnpivotMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var fixColumns = FixColumns.Length > 0
                ? new HashSet<string>(FixColumns)
                : null;

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                foreach (var cell in row.Values)
                {
                    if (cell.Value == null && IgnoreIfValueIsNull)
                        continue;

                    if (fixColumns?.Contains(cell.Key) == true)
                        continue;

                    var initialValues = FixColumns.Select(x => new KeyValuePair<string, object>(x, row[x])).ToList();
                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForDimension, cell.Key));
                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForValue, cell.Value));

                    var newRow = Context.CreateRow(this, initialValues);
                    yield return newRow;
                }

                Context.SetRowOwner(row, null);
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (FixColumns == null)
                throw new ProcessParameterNullException(this, nameof(FixColumns));

            if (NewColumnForValue == null)
                throw new ProcessParameterNullException(this, nameof(NewColumnForValue));

            if (NewColumnForDimension == null)
                throw new ProcessParameterNullException(this, nameof(NewColumnForDimension));
        }
    }
}