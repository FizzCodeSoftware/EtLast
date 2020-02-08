namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class TrimStringMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }

        protected TrimStringMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

                var columns = Columns ?? row.Values.Select(x => x.Key).ToArray();
                foreach (var column in columns)
                {
                    var source = row[column];
                    if (source is string str && !string.IsNullOrEmpty(str))
                    {
                        var trimmed = str.Trim();
                        if (trimmed != str)
                        {
                            row.SetValue(column, trimmed, this);
                        }
                    }
                }

                yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));
        }
    }
}