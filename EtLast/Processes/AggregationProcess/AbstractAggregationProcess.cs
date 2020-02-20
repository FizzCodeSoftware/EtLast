namespace FizzCode.EtLast
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;

    public abstract class AbstractAggregationProcess : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string[] GroupingColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        protected AbstractAggregationProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected string GenerateKey(IRow row)
        {
            _keyBuilder.Clear();
            for (var i = 0; i < GroupingColumns.Length; i++)
            {
                var v = row[GroupingColumns[i]];
                _keyBuilder
                    .Append(v != null ? v.ToString() : "NULL")
                    .Append("|");
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