namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class ReplaceNullWithValueMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }
        public object Value { get; set; }

        public ReplaceNullWithValueMutator(IEtlContext context, string name, string topic)
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

                var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();
                foreach (var column in Columns)
                {
                    if (row.IsNull(column))
                    {
                        row.SetValue(column, Value, this);
                    }
                }

                yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (Value == null)
                throw new ProcessParameterNullException(this, nameof(Value));
        }
    }
}