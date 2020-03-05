namespace FizzCode.EtLast
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;

    public abstract class AbstractAggregationMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public List<ColumnCopyConfiguration> GroupingColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        protected AbstractAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected string GetKey(IReadOnlySlimRow row)
        {
            if (GroupingColumns.Count == 1)
            {
                var col = GroupingColumns[0];
                return !row.IsNull(col.FromColumn)
                    ? row.FormatToString(col.FromColumn)
                    : "\0";
            }

            _keyBuilder.Clear();
            for (var i = 0; i < GroupingColumns.Count; i++)
            {
                var col = GroupingColumns[i];
                if (!row.IsNull(col.FromColumn))
                    _keyBuilder.Append(row.FormatToString(col.FromColumn));

                _keyBuilder.Append('\0');
            }

            return _keyBuilder.ToString();
        }

        public IEnumerator<IMutator> GetEnumerator()
        {
            yield return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            yield return this;
        }
    }
}