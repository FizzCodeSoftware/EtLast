namespace FizzCode.EtLast
{
    public class MergeStringColumnsOperation : AbstractRowOperation
    {
        public IfRowDelegate If { get; set; }
        public string[] ColumnsToMerge { get; set; }
        public string TargetColumn { get; set; }
        public string Separator { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            string newValue = null;

            foreach (var column in ColumnsToMerge)
            {
                var value = row.GetAs<string>(column);
                if (!string.IsNullOrEmpty(value))
                {
                    newValue += (newValue == null ? "" : Separator) + value;
                }

                row.RemoveColumn(column, this);
            }

            row.SetValue(TargetColumn, newValue, this);
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(TargetColumn))
                throw new OperationParameterNullException(this, nameof(TargetColumn));
            if (ColumnsToMerge == null || ColumnsToMerge.Length == 0)
                throw new OperationParameterNullException(this, nameof(ColumnsToMerge));
        }
    }
}