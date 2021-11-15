namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;

    public sealed class UnpivotMutator : AbstractMutator
    {
        public List<ColumnCopyConfiguration> FixColumns { get; init; }
        public string NewColumnForDimension { get; init; }
        public string NewColumnForValue { get; init; }

        /// <summary>
        /// Default value is true.
        /// </summary>
        public bool IgnoreIfValueIsNull { get; init; } = true;

        /// <summary>
        /// Default value is true;
        /// </summary>
        public bool CopyTag { get; init; } = true;

        public string[] ValueColumns { get; init; }

        private HashSet<string> _fixColumnNames;
        private HashSet<string> _valueColumnNames;

        public UnpivotMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void StartMutator()
        {
            _fixColumnNames = FixColumns != null
                ? new HashSet<string>(FixColumns.Select(x => x.FromColumn))
                : new HashSet<string>();

            _valueColumnNames = ValueColumns != null
                ? new HashSet<string>(ValueColumns)
                : new HashSet<string>();
        }

        protected override void CloseMutator()
        {
            _fixColumnNames.Clear();
            _fixColumnNames = null;
            _valueColumnNames.Clear();
            _valueColumnNames = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (ValueColumns == null)
            {
                foreach (var kvp in row.Values)
                {
                    if (_fixColumnNames.Contains(kvp.Key))
                        continue;

                    var initialValues = FixColumns.ConvertAll(x => new KeyValuePair<string, object>(x.ToColumn, row[x.FromColumn]));
                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForDimension, kvp.Key));
                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForValue, kvp.Value));

                    var newRow = Context.CreateRow(this, initialValues);

                    if (CopyTag)
                        newRow.Tag = row.Tag;

                    yield return newRow;
                }
            }
            else
            {
                foreach (var col in ValueColumns)
                {
                    var value = row[col];
                    if (value == null && IgnoreIfValueIsNull)
                        continue;

                    var initialValues = FixColumns != null
                        ? FixColumns.ConvertAll(x => new KeyValuePair<string, object>(x.ToColumn, row[x.FromColumn]))
                        : row.Values.Where(kvp => !_valueColumnNames.Contains(kvp.Key)).ToList();

                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForDimension, col));
                    initialValues.Add(new KeyValuePair<string, object>(NewColumnForValue, value));

                    var newRow = Context.CreateRow(this, initialValues);

                    if (CopyTag)
                        newRow.Tag = row.Tag;

                    yield return newRow;
                }
            }
        }

        protected override void ValidateMutator()
        {
            if (ValueColumns == null && FixColumns == null)
                throw new InvalidProcessParameterException(this, nameof(ValueColumns), null, "if " + nameof(ValueColumns) + " is null then " + nameof(FixColumns) + " must be set");

            if (NewColumnForValue == null)
                throw new ProcessParameterNullException(this, nameof(NewColumnForValue));

            if (NewColumnForDimension == null)
                throw new ProcessParameterNullException(this, nameof(NewColumnForDimension));

            if (!IgnoreIfValueIsNull && ValueColumns == null)
                throw new InvalidProcessParameterException(this, nameof(ValueColumns), null, "if " + nameof(IgnoreIfValueIsNull) + " is false then " + nameof(ValueColumns) + " must be set");
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class UnpivotMutatorFluent
    {
        public static IFluentProcessMutatorBuilder Unpivot(this IFluentProcessMutatorBuilder builder, UnpivotMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}