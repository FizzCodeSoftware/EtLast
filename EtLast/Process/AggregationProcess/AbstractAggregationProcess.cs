namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Text;

    public abstract class AbstractAggregationProcess : IProcess
    {
        public IEtlContext Context { get; }
        public string Name { get; set; }

        public IProcess Caller { get; protected set; }
        public IProcess InputProcess { get; set; }

        public string[] GroupingColumns { get; set; }

        private readonly StringBuilder _keyBuilder = new StringBuilder();

        protected AbstractAggregationProcess(IEtlContext context, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public abstract IEnumerable<IRow> Evaluate(IProcess caller = null);

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