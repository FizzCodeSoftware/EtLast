namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using System.Text;

    public sealed class MergeStringColumnsMutator : AbstractSimpleChangeMutator
    {
        public string[] ColumnsToMerge { get; set; }
        public string TargetColumn { get; set; }
        public string Separator { get; set; }

        private readonly StringBuilder _sb = new();

        public MergeStringColumnsMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void StartMutator()
        {
            base.StartMutator();

            Changes.AddRange(ColumnsToMerge.Select(x => new KeyValuePair<string, object>(x, null)));
            Changes.Add(new KeyValuePair<string, object>(TargetColumn, null));
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
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
            }

            Changes[ColumnsToMerge.Length] = new KeyValuePair<string, object>(TargetColumn, _sb.ToString());
            _sb.Clear();

            row.MergeWith(Changes);

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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class MergeStringColumnsMutatorFluent
    {
        public static IFluentProcessMutatorBuilder MergeStringColumns(this IFluentProcessMutatorBuilder builder, MergeStringColumnsMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}