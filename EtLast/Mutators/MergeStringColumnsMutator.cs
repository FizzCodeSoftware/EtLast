namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Text;

    public class MergeStringColumnsMutator : AbstractMutator
    {
        public string[] ColumnsToMerge { get; set; }
        public string TargetColumn { get; set; }
        public string Separator { get; set; }

        private readonly StringBuilder _sb = new StringBuilder();

        public MergeStringColumnsMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
        {
            foreach (var column in ColumnsToMerge)
            {
                if (_sb.Length > 0)
                    _sb.Append(Separator);

                var value = row.GetAs<string>(column, null);
                if (!string.IsNullOrEmpty(value))
                {
                    _sb.Append(value);
                }

                row.SetStagedValue(column, null);
            }

            row.SetStagedValue(TargetColumn, _sb.ToString());
            _sb.Clear();

            row.ApplyStaging();

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(TargetColumn))
                throw new ProcessParameterNullException(this, nameof(TargetColumn));

            if (ColumnsToMerge == null || ColumnsToMerge.Length == 0)
                throw new ProcessParameterNullException(this, nameof(ColumnsToMerge));
        }
    }
}