namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ReplaceEmptyStringWithNullMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }

        public ReplaceEmptyStringWithNullMutator(IEtlContext context, string name, string topic)
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
#pragma warning disable CA1820 // Test for empty strings using string length
                    if (string.Equals(row.GetAs<string>(column, null), string.Empty, StringComparison.InvariantCultureIgnoreCase))
#pragma warning restore CA1820 // Test for empty strings using string length
                    {
                        row.SetValue(column, null, this);
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