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
                var value = row[GroupingColumns[0].FromColumn];

                return value != null
                    ? DefaultValueFormatter.Format(value)
                    : "\0";
            }

            _keyBuilder.Clear();
            for (var i = 0; i < GroupingColumns.Count; i++)
            {
                var value = row[GroupingColumns[i].FromColumn];

                if (value != null)
                    _keyBuilder.Append(DefaultValueFormatter.Format(value));

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