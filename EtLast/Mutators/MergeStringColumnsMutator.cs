namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Text;

    public class MergeStringColumnsMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string[] ColumnsToMerge { get; set; }
        public string TargetColumn { get; set; }
        public string Separator { get; set; }

        public MergeStringColumnsMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var sb = new StringBuilder();

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

                foreach (var column in ColumnsToMerge)
                {
                    if (sb.Length > 0)
                        sb.Append(Separator);

                    var value = row.GetAs<string>(column, null);
                    if (!string.IsNullOrEmpty(value))
                    {
                        sb.Append(value);
                    }

                    row.SetValue(column, null, this);
                }

                row.SetValue(TargetColumn, sb.ToString(), this);
                sb.Clear();

                yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (string.IsNullOrEmpty(TargetColumn))
                throw new ProcessParameterNullException(this, nameof(TargetColumn));

            if (ColumnsToMerge == null || ColumnsToMerge.Length == 0)
                throw new ProcessParameterNullException(this, nameof(ColumnsToMerge));
        }
    }
}