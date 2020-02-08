namespace FizzCode.EtLast
{
    using System.Text;

    public abstract class AbstractAggregationProcess : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string[] GroupingColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        protected AbstractAggregationProcess(IEtlContext context, string name, string topic)
            : base(context, name, topic)
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
    }
}