namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ColumnValidationMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public string Column { get; set; }
        public Func<IRow, bool> ErrorIf { get; set; }

        protected ColumnValidationMutator(IEtlContext context, string name, string topic)
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

                CounterCollection.IncrementCounter("executed", 1);

                if (ErrorIf(row))
                {
                    row.SetValue(Column, new EtlRowError()
                    {
                        Process = this,
                        OriginalValue = row[Column],
                        Message = "validation failed",
                    }, this);
                }

                yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (string.IsNullOrEmpty(Column))
                throw new ProcessParameterNullException(this, nameof(Column));

            if (ErrorIf == null)
                throw new ProcessParameterNullException(this, nameof(ErrorIf));
        }
    }
}