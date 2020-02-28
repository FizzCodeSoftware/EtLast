namespace FizzCode.EtLast
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;

    public abstract class AbstractAggregationMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string[] GroupingColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        protected AbstractAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected string GetKey(IRow row)
        {
            if (GroupingColumns.Length == 1)
            {
                var col = GroupingColumns[0];
                return !row.IsNull(col) ? row.FormatToString(col) : "\0";
            }

            _keyBuilder.Clear();
            for (var i = 0; i < GroupingColumns.Length; i++)
            {
                var col = GroupingColumns[i];
                if (!row.IsNull(col))
                    _keyBuilder.Append(row.FormatToString(col));

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